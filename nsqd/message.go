package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	// for in-flight handling
	deliveryTS time.Time
	clientID   int64
	pri        int64
	index      int
	deferred   time.Duration
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
//
//	[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
//	|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
//	|       8-byte         ||    ||                 16-byte                      || N-byte
//	------------------------------------------------------------------------------------------...
//	  nanosecond timestamp    ^^                   message ID                       message body
//	                       (uint16)
//	                        2-byte
//	                       attempts
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}

// deferredMsgHeaderLength is the size of the prefix added to a deferred
// message when it is persisted: an 8-byte big-endian absolute expiry
// timestamp (UnixNano) at which the message should become deliverable.
const deferredMsgHeaderLength = 8

// writeDeferredMessage serializes a deferred message as its absolute expiry
// timestamp followed by the standard message serialization.
//
// Storing the absolute expiry (rather than a relative duration) ensures the
// original schedule survives a restart verbatim and is never recomputed
// from the restart time.
func writeDeferredMessage(w io.Writer, msg *Message, deferredTs int64) (int64, error) {
	var buf [deferredMsgHeaderLength]byte
	binary.BigEndian.PutUint64(buf[:], uint64(deferredTs))

	n, err := w.Write(buf[:])
	total := int64(n)
	if err != nil {
		return total, err
	}

	m, err := msg.WriteTo(w)
	total += m
	return total, err
}

// decodeDeferredMessage deserializes data (as []byte) produced by
// writeDeferredMessage, returning the message and its absolute expiry
// timestamp (UnixNano).
func decodeDeferredMessage(b []byte) (*Message, int64, error) {
	if len(b) < deferredMsgHeaderLength+minValidMsgLength {
		return nil, 0, fmt.Errorf("invalid deferred message buffer size (%d)", len(b))
	}

	deferredTs := int64(binary.BigEndian.Uint64(b[:deferredMsgHeaderLength]))
	msg, err := decodeMessage(b[deferredMsgHeaderLength:])
	if err != nil {
		return nil, 0, err
	}

	return msg, deferredTs, nil
}

func writeMessageToBackend(msg *Message, bq BackendQueue) error {
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}
