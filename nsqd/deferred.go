package nsqd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/nsqio/nsq/internal/pqueue"
)

// Deferred messages (from DPUB or a deferred REQ) are persisted per channel
// so that a clean nsqd restart does not lose their remaining delay.
//
// The file stores the ABSOLUTE expiration timestamp (UnixNano) of each
// message next to the standard message encoding. The absolute timestamp is
// the authoritative value: after a restart the remaining delay is derived
// from it (expiry - now), it is never restarted from zero.
//
// File layout:
//
//	[4]byte  magic "NSQD"
//	uint16   format version (1)
//	repeated records:
//	  int64  absolute expiration timestamp (UnixNano)
//	  int32  message length in bytes
//	  []byte message (standard Message.WriteTo encoding)
const (
	deferredFileMagic   = "NSQD"
	deferredFileVersion = 1
)

type deferredRecord struct {
	expiresAt int64 // absolute expiration, UnixNano
	msg       *Message
}

// writeDeferredFile atomically persists the given deferred queue items
// (each item's Priority is the absolute expiration timestamp).
func writeDeferredFile(fileName string, items []*pqueue.Item) (err error) {
	tmpName := fileName + ".tmp"
	f, err := os.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmpName)
		}
	}()

	w := bufio.NewWriter(f)

	var hdr [6]byte
	copy(hdr[:4], deferredFileMagic)
	binary.BigEndian.PutUint16(hdr[4:], deferredFileVersion)
	if _, err = w.Write(hdr[:]); err != nil {
		return err
	}

	var recHdr [12]byte
	for _, item := range items {
		msg := item.Value.(*Message)
		binary.BigEndian.PutUint64(recHdr[:8], uint64(item.Priority))
		binary.BigEndian.PutUint32(recHdr[8:], uint32(minValidMsgLength+len(msg.Body)))
		if _, err = w.Write(recHdr[:]); err != nil {
			return err
		}
		if _, err = msg.WriteTo(w); err != nil {
			return err
		}
	}

	if err = w.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	// os.Rename fails on Windows if the destination exists
	_ = os.Remove(fileName)
	return os.Rename(tmpName, fileName)
}

// readDeferredFile reads back records persisted by writeDeferredFile.
// A missing file is not an error. If the file is corrupt the records
// decoded so far are returned along with the error.
func readDeferredFile(fileName string, maxMsgSize int32) ([]deferredRecord, error) {
	f, err := os.Open(fileName)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	var hdr [6]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("invalid header: %w", err)
	}
	if string(hdr[:4]) != deferredFileMagic ||
		binary.BigEndian.Uint16(hdr[4:]) != deferredFileVersion {
		return nil, fmt.Errorf("invalid magic or version")
	}

	var records []deferredRecord
	var recHdr [12]byte
	for {
		_, err := io.ReadFull(r, recHdr[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, fmt.Errorf("corrupt record header: %w", err)
		}
		expiresAt := int64(binary.BigEndian.Uint64(recHdr[:8]))
		msgLen := int32(binary.BigEndian.Uint32(recHdr[8:]))
		if msgLen < minValidMsgLength || msgLen > maxMsgSize {
			return records, fmt.Errorf("invalid message size %d", msgLen)
		}
		buf := make([]byte, msgLen)
		if _, err := io.ReadFull(r, buf); err != nil {
			return records, fmt.Errorf("corrupt message: %w", err)
		}
		msg, err := decodeMessage(buf)
		if err != nil {
			return records, err
		}
		records = append(records, deferredRecord{expiresAt: expiresAt, msg: msg})
	}
	return records, nil
}
