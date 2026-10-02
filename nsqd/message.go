package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

// quarantineSourceFlag is the high bit of the on-disk Attempts field. When
// set, the message carries a quarantine source ("topic:channel") between the
// message ID and the body. Messages without a quarantine source serialize
// byte-identical to the legacy format.
const quarantineSourceFlag = 0x8000

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	// QuarantineSource records the "topic:channel" a message was
	// quarantined from. Empty for normal messages. It is persisted by
	// WriteToStore but is not part of the client wire format (WriteTo).
	QuarantineSource string

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

// WriteToStore serializes the message for the backend queue. It is identical
// to WriteTo for normal messages; messages with a QuarantineSource use an
// extended layout (flagged via the high bit of the attempts field) so the
// failure source survives restarts. The client wire format (WriteTo) is
// intentionally left unchanged.
func (m *Message) WriteToStore(w io.Writer) (int64, error) {
	if m.QuarantineSource == "" {
		return m.WriteTo(w)
	}

	var total int64
	src := []byte(m.QuarantineSource)
	if len(src) > math.MaxUint16 {
		return total, fmt.Errorf("quarantine source too long (%d bytes)", len(src))
	}

	buf := make([]byte, 10+MsgIDLength+2+len(src))
	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], (m.Attempts&0x7fff)|quarantineSourceFlag)
	copy(buf[10:10+MsgIDLength], m.ID[:])
	binary.BigEndian.PutUint16(buf[10+MsgIDLength:12+MsgIDLength], uint16(len(src)))
	copy(buf[12+MsgIDLength:], src)

	n, err := w.Write(buf)
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	return total, err
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
//
// when the high bit of attempts is set the message carries a quarantine
// source and the layout becomes:
//
//	| timestamp || attempts|flag || message ID || (uint16) len || source || body |
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	hasQuarantineSource := msg.Attempts&quarantineSourceFlag != 0
	msg.Attempts &^= quarantineSourceFlag
	copy(msg.ID[:], b[10:10+MsgIDLength])
	body := b[10+MsgIDLength:]

	if hasQuarantineSource {
		if len(body) < 2 {
			return nil, fmt.Errorf("invalid quarantined message buffer size (%d)", len(b))
		}
		srcLen := int(binary.BigEndian.Uint16(body[:2]))
		if len(body) < 2+srcLen {
			return nil, fmt.Errorf("invalid quarantine source length (%d)", srcLen)
		}
		msg.QuarantineSource = string(body[2 : 2+srcLen])
		body = body[2+srcLen:]
	}
	msg.Body = body

	return &msg, nil
}

func writeMessageToBackend(msg *Message, bq BackendQueue) error {
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)
	_, err := msg.WriteToStore(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}
