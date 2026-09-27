package nsqd

import (
	"bytes"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

func TestDeferredMessageCodec(t *testing.T) {
	var id MessageID
	copy(id[:], "0123456789abcdef")
	msg := NewMessage(id, []byte("deferred body"))
	msg.Attempts = 3
	deferredTs := time.Now().Add(time.Hour).UnixNano()

	var buf bytes.Buffer
	_, err := writeDeferredMessage(&buf, msg, deferredTs)
	test.Nil(t, err)

	decodedMsg, decodedTs, err := decodeDeferredMessage(buf.Bytes())
	test.Nil(t, err)
	test.Equal(t, deferredTs, decodedTs)
	test.Equal(t, msg.ID, decodedMsg.ID)
	test.Equal(t, msg.Timestamp, decodedMsg.Timestamp)
	test.Equal(t, msg.Attempts, decodedMsg.Attempts)
	test.Equal(t, msg.Body, decodedMsg.Body)

	// truncated buffers must be rejected
	_, _, err = decodeDeferredMessage(buf.Bytes()[:deferredMsgHeaderLength+2])
	test.NotNil(t, err)
	_, _, err = decodeDeferredMessage(nil)
	test.NotNil(t, err)

	// the standard (pre-existing) on-disk format must still decode, so
	// messages written by older nsqd versions remain readable
	var oldBuf bytes.Buffer
	_, err = msg.WriteTo(&oldBuf)
	test.Nil(t, err)
	oldMsg, err := decodeMessage(oldBuf.Bytes())
	test.Nil(t, err)
	test.Equal(t, msg.ID, oldMsg.ID)
	test.Equal(t, msg.Body, oldMsg.Body)
}

// TestDeferredPersistenceAcrossRestart exercises the full restart scenario:
//
//   - msgA (DPUB path): deferred, expires while nsqd is stopped
//   - msgB (REQ path): deferred requeue, expires after the restart
//   - msgC: already promoted from the deferred queue to the deliverable
//     queue when nsqd stops
//
// After the restart no message may be lost or duplicated, and the deferred
// expiry of msgB must be restored verbatim (not recomputed from zero).
func TestDeferredPersistenceAcrossRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nsq-deferred-test-")
	test.Nil(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	newOpts := func() *Options {
		opts := NewOptions()
		opts.Logger = test.NewTestLogger(t)
		opts.DataPath = tmpDir
		opts.QueueScanInterval = 50 * time.Millisecond
		opts.QueueScanRefreshInterval = 50 * time.Millisecond
		return opts
	}

	topicName := "test_deferred_restart" + strconv.Itoa(int(time.Now().Unix()))

	_, _, nsqd := mustStartNSQD(newOpts())
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")

	// msgA: deferred publish (DPUB path), expires during downtime
	msgA := NewMessage(topic.GenerateID(), []byte("msgA"))
	channel.PutMessageDeferred(msgA, 1500*time.Millisecond)

	// msgB: deferred requeue (REQ path), expires after the restart
	msgB := NewMessage(topic.GenerateID(), []byte("msgB"))
	err = channel.StartInFlightTimeout(msgB, 0, 60*time.Second)
	test.Nil(t, err)
	err = channel.RequeueMessage(0, msgB.ID, time.Hour)
	test.Nil(t, err)

	channel.deferredMutex.Lock()
	msgBOriginalExpiry := channel.deferredMessages[msgB.ID].Priority
	channel.deferredMutex.Unlock()

	// msgC: deferred briefly and promoted to the deliverable queue before
	// the shutdown (in transition between the two queues)
	msgC := NewMessage(topic.GenerateID(), []byte("msgC"))
	channel.PutMessageDeferred(msgC, 100*time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for len(channel.deferredMessages) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("msgC was not promoted to the deliverable queue in time")
		}
		time.Sleep(20 * time.Millisecond)
	}

	nsqd.Exit()

	// msgA expires while nsqd is down
	time.Sleep(2 * time.Second)

	_, _, nsqd = mustStartNSQD(newOpts())
	defer nsqd.Exit()

	topic = nsqd.GetTopic(topicName)
	channel = topic.GetChannel("ch")

	// msgB must be restored as deferred with its original absolute expiry,
	// not recomputed from the restart time
	channel.deferredMutex.Lock()
	item, ok := channel.deferredMessages[msgB.ID]
	channel.deferredMutex.Unlock()
	test.Equal(t, true, ok)
	test.Equal(t, msgBOriginalExpiry, item.Priority)

	// msgA expired during downtime and must be promoted to the deliverable
	// queue shortly after startup
	select {
	case msg := <-channel.memoryMsgChan:
		test.Equal(t, msgA.ID, msg.ID)
		test.Equal(t, []byte("msgA"), msg.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("msgA was not promoted to the deliverable queue after restart")
	}

	// msgC was flushed to the backend queue while in transition and must be
	// recoverable exactly once
	select {
	case buf := <-channel.backend.ReadChan():
		msg, err := decodeMessage(buf)
		test.Nil(t, err)
		test.Equal(t, msgC.ID, msg.ID)
		test.Equal(t, []byte("msgC"), msg.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("msgC was not recovered from the backend queue after restart")
	}

	// nothing lost, nothing duplicated: msgB still deferred, everything
	// else consumed
	test.Equal(t, 0, len(channel.memoryMsgChan))
	test.Equal(t, int64(0), channel.backend.Depth())
	test.Equal(t, 1, len(channel.deferredMessages))
}

// TestDeferredBackwardCompatibleDiskData ensures that disk data written by
// an older nsqd (which flushed deferred messages into the main backend
// queue without their expiry) is still readable.
func TestDeferredBackwardCompatibleDiskData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nsq-deferred-compat-test-")
	test.Nil(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	newOpts := func() *Options {
		opts := NewOptions()
		opts.Logger = test.NewTestLogger(t)
		opts.DataPath = tmpDir
		return opts
	}

	topicName := "test_deferred_compat" + strconv.Itoa(int(time.Now().Unix()))

	_, _, nsqd := mustStartNSQD(newOpts())
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")

	// simulate old-format disk data: a deferred message flushed to the main
	// backend queue by a previous nsqd version (standard message format, no
	// expiry timestamp, no deferred file)
	msg := NewMessage(topic.GenerateID(), []byte("old-format"))
	err = writeMessageToBackend(msg, channel.backend)
	test.Nil(t, err)

	nsqd.Exit()

	_, _, nsqd = mustStartNSQD(newOpts())
	defer nsqd.Exit()

	topic = nsqd.GetTopic(topicName)
	channel = topic.GetChannel("ch")

	select {
	case buf := <-channel.backend.ReadChan():
		decodedMsg, err := decodeMessage(buf)
		test.Nil(t, err)
		test.Equal(t, msg.ID, decodedMsg.ID)
		test.Equal(t, []byte("old-format"), decodedMsg.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("old-format message was not recovered after restart")
	}
	test.Equal(t, 0, len(channel.deferredMessages))
}
