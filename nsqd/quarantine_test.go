package nsqd

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

// quarantined messages must survive a backend round-trip with their failure
// source and attempt count, while normal messages keep the legacy byte layout
func TestQuarantineMessageSerialization(t *testing.T) {
	var id MessageID
	copy(id[:], "1234567890abcdef")
	body := []byte("hello")
	msg := NewMessage(id, body)
	msg.Attempts = 7

	// normal messages serialize byte-identical to the legacy format
	var buf bytes.Buffer
	_, err := msg.WriteToStore(&buf)
	test.Nil(t, err)
	var legacy bytes.Buffer
	_, err = msg.WriteTo(&legacy)
	test.Nil(t, err)
	test.Equal(t, legacy.Bytes(), buf.Bytes())

	dmsg, err := decodeMessage(buf.Bytes())
	test.Nil(t, err)
	test.Equal(t, "", dmsg.QuarantineSource)
	test.Equal(t, uint16(7), dmsg.Attempts)
	test.Equal(t, id, dmsg.ID)
	test.Equal(t, body, dmsg.Body)

	// quarantined messages round-trip the failure source and count
	msg.QuarantineSource = "topic:channel"
	buf.Reset()
	_, err = msg.WriteToStore(&buf)
	test.Nil(t, err)
	dmsg, err = decodeMessage(buf.Bytes())
	test.Nil(t, err)
	test.Equal(t, "topic:channel", dmsg.QuarantineSource)
	test.Equal(t, uint16(7), dmsg.Attempts)
	test.Equal(t, id, dmsg.ID)
	test.Equal(t, body, dmsg.Body)

	// the client wire format (WriteTo) stays legacy even for quarantined
	// messages, so existing clients can consume quarantine channels
	legacy.Reset()
	_, err = msg.WriteTo(&legacy)
	test.Nil(t, err)
	dmsg, err = decodeMessage(legacy.Bytes())
	test.Nil(t, err)
	test.Equal(t, "", dmsg.QuarantineSource)
	test.Equal(t, uint16(7), dmsg.Attempts)
	test.Equal(t, id, dmsg.ID)
	test.Equal(t, body, dmsg.Body)
}

// messages below the max delivery count requeue normally; reaching the max
// moves the original message (ID, body, attempts) to the quarantine target
// and records the failure source
func TestQuarantineAfterMaxDeliveries(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_quarantine_max" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	dlq := topic.GetChannel("dlq")

	err := channel.SetQuarantine(2, "dlq")
	test.Nil(t, err)

	// below the max: REQ behaves exactly as before
	msg1 := NewMessage(topic.GenerateID(), []byte("one"))
	msg1.Attempts = 1
	err = channel.StartInFlightTimeout(msg1, 0, opts.MsgTimeout)
	test.Nil(t, err)
	err = channel.RequeueMessage(0, msg1.ID, 0)
	test.Nil(t, err)
	test.Equal(t, 0, len(channel.inFlightMessages))
	test.Equal(t, 1, len(channel.memoryMsgChan))
	test.Equal(t, 0, len(dlq.memoryMsgChan))
	requeued := <-channel.memoryMsgChan
	test.Equal(t, msg1.ID, requeued.ID)
	test.Equal(t, "", requeued.QuarantineSource)

	// reaching the max: the message is quarantined instead of requeued
	msg2 := NewMessage(topic.GenerateID(), []byte("two"))
	msg2.Attempts = 2
	err = channel.StartInFlightTimeout(msg2, 0, opts.MsgTimeout)
	test.Nil(t, err)
	err = channel.RequeueMessage(0, msg2.ID, 0)
	test.Nil(t, err)
	test.Equal(t, 0, len(channel.inFlightMessages))
	test.Equal(t, 0, len(channel.memoryMsgChan))
	test.Equal(t, 1, len(dlq.memoryMsgChan))
	test.Equal(t, uint64(1), atomic.LoadUint64(&channel.quarantineCount))

	qmsg := <-dlq.memoryMsgChan
	test.Equal(t, msg2.ID, qmsg.ID)
	test.Equal(t, []byte("two"), qmsg.Body)
	test.Equal(t, uint16(2), qmsg.Attempts)
	test.Equal(t, topicName+":ch", qmsg.QuarantineSource)
}

// quarantine target chains that would form a cycle must be rejected
func TestQuarantineCycleRejected(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_quarantine_cycle" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	chA := topic.GetChannel("a")
	chB := topic.GetChannel("b")
	chC := topic.GetChannel("c")

	err := chA.SetQuarantine(2, "b")
	test.Nil(t, err)
	err = chB.SetQuarantine(2, "c")
	test.Nil(t, err)
	// c -> a would close the loop a -> b -> c -> a
	err = chC.SetQuarantine(2, "a")
	test.NotNil(t, err)
	// a channel cannot quarantine to itself
	err = chA.SetQuarantine(2, "a")
	test.NotNil(t, err)
	// cross-topic chains are followed as well
	topic2 := nsqd.GetTopic(topicName + "2")
	chD := topic2.GetChannel("d")
	err = chD.SetQuarantine(2, topicName+":a")
	test.Nil(t, err)
	// a -> t2:d -> t:a would close a cross-topic loop
	err = chA.SetQuarantine(2, topic2.name+":d")
	test.NotNil(t, err)
	// invalid targets and limits are rejected
	err = chA.SetQuarantine(0, "b")
	test.NotNil(t, err)
	err = chA.SetQuarantine(2, "too:many:parts")
	test.NotNil(t, err)
	err = chA.SetQuarantine(2, "invalid name!")
	test.NotNil(t, err)

	// the rejected configs must not have been applied
	_, target := chC.QuarantineConfig()
	test.Equal(t, "", target)
	_, target = chA.QuarantineConfig()
	test.Equal(t, topicName+":b", target)
}

// fault injection, crash ordering 1: the quarantine target write completed
// but the source channel never removed the message (process crash in
// between). The message must not be lost; at-least-once semantics allow a
// duplicate in the target after recovery.
func TestQuarantineCrashAfterTargetWrite(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_quarantine_crash1" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	dlq := topic.GetChannel("dlq")

	err := channel.SetQuarantine(2, "dlq")
	test.Nil(t, err)

	msg := NewMessage(topic.GenerateID(), []byte("payload"))
	msg.Attempts = 2
	err = channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
	test.Nil(t, err)

	// fault injection: perform only the first phase of the migration
	// (target write), simulating a crash before the source removal
	err = channel.writeToQuarantineTarget(msg)
	test.Nil(t, err)

	// the message exists in both places: the source copy is still tracked
	// in-flight, so nothing was silently dropped
	test.Equal(t, 1, len(channel.inFlightMessages))
	test.Equal(t, 1, len(dlq.memoryMsgChan))

	// recovery: the source message is redelivered (in-flight messages are
	// requeued on restart), fails again, and the migration is retried
	err = channel.RequeueMessage(0, msg.ID, 0)
	test.Nil(t, err)

	// at-least-once: a duplicate in the target is acceptable, silent loss
	// is not
	test.Equal(t, 0, len(channel.inFlightMessages))
	test.Equal(t, 2, len(dlq.memoryMsgChan))
	test.Equal(t, uint64(1), atomic.LoadUint64(&channel.quarantineCount))

	for i := 0; i < 2; i++ {
		qmsg := <-dlq.memoryMsgChan
		test.Equal(t, msg.ID, qmsg.ID)
		test.Equal(t, []byte("payload"), qmsg.Body)
		test.Equal(t, uint16(2), qmsg.Attempts)
		test.Equal(t, topicName+":ch", qmsg.QuarantineSource)
	}
}

// fault injection, crash ordering 2: the target write fails (target
// unavailable). The source message must NOT be finished; it stays in-flight
// and the migration can be retried once the target recovers.
func TestQuarantineCrashBeforeTargetWrite(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_quarantine_crash2" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	dlq := topic.GetChannel("dlq")

	err := channel.SetQuarantine(2, "dlq")
	test.Nil(t, err)

	// make the quarantine target unavailable
	err = dlq.Close()
	test.Nil(t, err)

	msg := NewMessage(topic.GenerateID(), []byte("payload"))
	msg.Attempts = 2
	err = channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
	test.Nil(t, err)

	// the migration fails and the source message is left untouched
	// (in-flight), never FIN'd
	err = channel.RequeueMessage(0, msg.ID, 0)
	test.NotNil(t, err)
	test.Equal(t, 1, len(channel.inFlightMessages))
	test.Equal(t, 1, len(channel.inFlightPQ))
	test.Equal(t, 0, len(dlq.memoryMsgChan))
	test.Equal(t, uint64(0), atomic.LoadUint64(&channel.quarantineCount))

	// once a healthy target is configured the migration succeeds
	err = channel.SetQuarantine(2, "dlq2")
	test.Nil(t, err)
	err = channel.RequeueMessage(0, msg.ID, 0)
	test.Nil(t, err)
	test.Equal(t, 0, len(channel.inFlightMessages))
	dlq2 := topic.GetChannel("dlq2")
	test.Equal(t, 1, len(dlq2.memoryMsgChan))
	qmsg := <-dlq2.memoryMsgChan
	test.Equal(t, msg.ID, qmsg.ID)
	test.Equal(t, []byte("payload"), qmsg.Body)
}

// messages that time out in-flight are also quarantined once they reach the
// max delivery count
func TestQuarantineInFlightTimeout(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MsgTimeout = 100 * time.Millisecond
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_quarantine_timeout" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	dlq := topic.GetChannel("dlq")

	err := channel.SetQuarantine(1, "dlq")
	test.Nil(t, err)

	msg := NewMessage(topic.GenerateID(), []byte("payload"))
	msg.Attempts = 1
	err = channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
	test.Nil(t, err)

	// the in-flight worker has a resolution of 100ms so we need to wait
	// at least that much longer than our msgTimeout (in worst case)
	time.Sleep(4 * opts.MsgTimeout)

	test.Equal(t, 0, len(channel.inFlightMessages))
	test.Equal(t, 1, len(dlq.memoryMsgChan))
	qmsg := <-dlq.memoryMsgChan
	test.Equal(t, msg.ID, qmsg.ID)
	test.Equal(t, topicName+":ch", qmsg.QuarantineSource)
}

// quarantine configuration must survive a restart via nsqd metadata
func TestQuarantineMetadataPersistence(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	dataPath := opts.DataPath
	defer func() { _ = os.RemoveAll(dataPath) }()

	topicName := "test_quarantine_meta" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	err := channel.SetQuarantine(3, "dlq")
	test.Nil(t, err)

	err = nsqd.PersistMetadata()
	test.Nil(t, err)
	nsqd.Exit()

	// restart with the same data path
	opts2 := NewOptions()
	opts2.Logger = test.NewTestLogger(t)
	opts2.DataPath = dataPath
	nsqd2, err := New(opts2)
	test.Nil(t, err)
	defer nsqd2.Exit()
	err = nsqd2.LoadMetadata()
	test.Nil(t, err)

	topic2, err := nsqd2.GetExistingTopic(topicName)
	test.Nil(t, err)
	channel2, err := topic2.GetExistingChannel("ch")
	test.Nil(t, err)
	maxDeliveries, target := channel2.QuarantineConfig()
	test.Equal(t, 3, maxDeliveries)
	test.Equal(t, topicName+":dlq", target)
}

// the /channel/quarantine HTTP endpoint configures, reports and clears the
// quarantine settings and rejects cycles
func TestQuarantineHTTP(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_quarantine_http" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	topic.GetChannel("ch")
	topic.GetChannel("dlq")

	url := func(ch string) string {
		return "http://" + httpAddr.String() + "/channel/quarantine?topic=" + topicName + "&channel=" + ch
	}
	post := func(ch, body string) int {
		resp, err := http.Post(url(ch), "application/json", bytes.NewBuffer([]byte(body)))
		test.Nil(t, err)
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.ReadAll(resp.Body)
		return resp.StatusCode
	}
	get := func(ch string) quarantineConfigV1 {
		resp, err := http.Get(url(ch))
		test.Nil(t, err)
		defer func() { _ = resp.Body.Close() }()
		var cfg quarantineConfigV1
		body, _ := io.ReadAll(resp.Body)
		err = json.Unmarshal(body, &cfg)
		test.Nil(t, err)
		return cfg
	}

	code := post("ch", `{"max_deliveries":3,"target":"dlq"}`)
	test.Equal(t, 200, code)
	cfg := get("ch")
	test.Equal(t, 3, cfg.MaxDeliveries)
	test.Equal(t, topicName+":dlq", cfg.Target)

	// dlq -> ch would create a cycle and must be rejected
	code = post("dlq", `{"max_deliveries":3,"target":"ch"}`)
	test.Equal(t, 400, code)
	cfg = get("dlq")
	test.Equal(t, "", cfg.Target)

	// invalid limits are rejected
	code = post("ch", `{"max_deliveries":0,"target":"dlq"}`)
	test.Equal(t, 400, code)

	// clearing restores default behavior
	req, err := http.NewRequest("DELETE", url("ch"), nil)
	test.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	test.Nil(t, err)
	_ = resp.Body.Close()
	test.Equal(t, 200, resp.StatusCode)
	cfg = get("ch")
	test.Equal(t, 0, cfg.MaxDeliveries)
	test.Equal(t, "", cfg.Target)
}
