package nsqd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	err := topic.PutMessage(msg)
	test.Nil(t, err)

	outputMsg := <-channel1.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg.ID)
	test.Equal(t, msg.Body, outputMsg.Body)
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	err := topic.PutMessage(msg)
	test.Nil(t, err)

	outputMsg1 := <-channel1.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg1.ID)
	test.Equal(t, msg.Body, outputMsg1.Body)

	outputMsg2 := <-channel2.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg2.ID)
	test.Equal(t, msg.Body, outputMsg2.Body)
}

func TestInFlightWorker(t *testing.T) {
	count := 250

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MsgTimeout = 100 * time.Millisecond
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_in_flight_worker" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		err := channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		test.Nil(t, err)
	}

	channel.Lock()
	inFlightMsgs := len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, count, inFlightMsgs)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs := len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, count, inFlightPQMsgs)

	// the in flight worker has a resolution of 100ms so we need to wait
	// at least that much longer than our msgTimeout (in worst case)
	time.Sleep(4 * opts.MsgTimeout)

	channel.Lock()
	inFlightMsgs = len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, 0, inFlightMsgs)

	channel.inFlightMutex.Lock()
	inFlightPQMsgs = len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, 0, inFlightPQMsgs)
}

func TestChannelEmpty(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 25)
	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		err := channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		test.Nil(t, err)
		msgs = append(msgs, msg)
	}

	err := channel.RequeueMessage(0, msgs[len(msgs)-1].ID, 100*time.Millisecond)
	test.Nil(t, err)
	test.Equal(t, 24, len(channel.inFlightMessages))
	test.Equal(t, 24, len(channel.inFlightPQ))
	test.Equal(t, 1, len(channel.deferredMessages))
	test.Equal(t, 1, len(channel.deferredPQ))

	err = channel.Empty()
	test.Nil(t, err)

	test.Equal(t, 0, len(channel.inFlightMessages))
	test.Equal(t, 0, len(channel.inFlightPQ))
	test.Equal(t, 0, len(channel.deferredMessages))
	test.Equal(t, 0, len(channel.deferredPQ))
	test.Equal(t, int64(0), channel.Depth())
}

func TestChannelEmptyConsumer(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer func() { _ = conn.Close() }()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")
	client := newClientV2(0, conn, nsqd)
	client.SetReadyCount(25)
	err := channel.AddClient(client.ID, client)
	test.Equal(t, err, nil)

	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		err = channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		test.Nil(t, err)
		client.SendingMessage()
	}

	for _, cl := range channel.clients {
		stats := cl.Stats("").(ClientV2Stats)
		test.Equal(t, int64(25), stats.InFlightCount)
	}

	err = channel.Empty()
	test.Nil(t, err)

	for _, cl := range channel.clients {
		stats := cl.Stats("").(ClientV2Stats)
		test.Equal(t, int64(0), stats.InFlightCount)
	}
}

func TestMaxChannelConsumers(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MaxChannelConsumers = 1
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer func() { _ = conn.Close() }()

	topicName := "test_max_channel_consumers" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	client1 := newClientV2(1, conn, nsqd)
	client1.SetReadyCount(25)
	err := channel.AddClient(client1.ID, client1)
	test.Equal(t, err, nil)

	client2 := newClientV2(2, conn, nsqd)
	client2.SetReadyCount(25)
	err = channel.AddClient(client2.ID, client2)
	test.NotEqual(t, err, nil)
}

func TestChannelHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 2

	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel := topic.GetChannel("channel")

	channel.backend = &errorBackendQueue{}

	msg := NewMessage(topic.GenerateID(), make([]byte, 100))
	err := channel.PutMessage(msg)
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.PutMessage(msg)
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.PutMessage(msg)
	test.NotNil(t, err)

	url := fmt.Sprintf("http://%s/ping", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 500, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	err = resp.Body.Close()
	test.Nil(t, err)
	test.Equal(t, "NOK - never gonna happen", string(body))

	channel.backend = &errorRecoveredBackendQueue{}

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.PutMessage(msg)
	test.Nil(t, err)

	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = io.ReadAll(resp.Body)
	err = resp.Body.Close()
	test.Nil(t, err)
	test.Equal(t, "OK", string(body))
}

func waitForEphemeralChannelDeletion(t *testing.T, topic *Topic, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		topic.RLock()
		_, ok := topic.channelMap[name]
		topic.RUnlock()
		if !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for ephemeral channel %s to be deleted", name)
		}
		time.Sleep(time.Millisecond)
	}
}

// an ephemeral channel with no consumers is deleted once the configured
// idle grace period elapses
func TestEphemeralChannelGracePeriodExpires(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.EphemeralChannelGracePeriod = 10 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_ephemeral_grace_expire" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channelName := "ch#ephemeral"
	channel := topic.GetChannel(channelName)

	client := newClientV2(0, nil, nsqd)
	err := channel.AddClient(client.ID, client)
	test.Nil(t, err)
	channel.RemoveClient(client.ID)

	waitForEphemeralChannelDeletion(t, topic, channelName)
	test.Equal(t, true, channel.Exiting())
}

// a consumer re-subscribing within the idle grace period cancels the
// pending deletion of an ephemeral channel
func TestEphemeralChannelGracePeriodResubscribe(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.EphemeralChannelGracePeriod = 10 * time.Millisecond

	expired := make(chan struct{})
	resume := make(chan struct{})
	deleted := make(chan struct{})
	var once sync.Once
	opts.ephemeralDeletionHook = func(c *Channel, proceed func()) {
		once.Do(func() { close(expired) })
		<-resume
		proceed()
		close(deleted)
	}

	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_ephemeral_grace_resub" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channelName := "ch#ephemeral"
	channel := topic.GetChannel(channelName)

	client1 := newClientV2(0, nil, nsqd)
	err := channel.AddClient(client1.ID, client1)
	test.Nil(t, err)
	channel.RemoveClient(client1.ID)

	// wait until the grace period has expired and the deletion is
	// precisely about to trigger
	<-expired

	// re-subscribe at the exact moment the deletion triggers
	conn, _ := mustConnectNSQD(tcpAddr)
	defer func() { _ = conn.Close() }()
	client2 := newClientV2(1, conn, nsqd)
	err = channel.AddClient(client2.ID, client2)
	test.Nil(t, err)
	close(resume)

	// the deletion attempt has now completed and must have been aborted
	<-deleted
	test.Equal(t, false, channel.Exiting())
	_, err = topic.GetExistingChannel(channelName)
	test.Nil(t, err)
}

// an explicit delete (followed by a same-name re-creation) takes
// precedence over a pending ephemeral deletion: the stale timer must not
// delete the new channel instance
func TestEphemeralChannelStaleTimerDoesNotDeleteNewInstance(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.EphemeralChannelGracePeriod = 10 * time.Millisecond

	expired := make(chan struct{})
	resume := make(chan struct{})
	deleted := make(chan struct{})
	var once sync.Once
	opts.ephemeralDeletionHook = func(c *Channel, proceed func()) {
		once.Do(func() { close(expired) })
		<-resume
		proceed()
		close(deleted)
	}

	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_ephemeral_stale_timer" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channelName := "ch#ephemeral"
	channel1 := topic.GetChannel(channelName)

	client1 := newClientV2(0, nil, nsqd)
	err := channel1.AddClient(client1.ID, client1)
	test.Nil(t, err)
	channel1.RemoveClient(client1.ID)

	// wait until the grace period has expired and the deletion of the
	// first channel instance is precisely about to trigger
	<-expired

	// an explicit delete wins over the pending timer...
	err = topic.DeleteExistingChannel(channelName)
	test.Nil(t, err)

	// ...and a new channel with the same name is created
	channel2 := topic.GetChannel(channelName)
	test.Equal(t, true, channel1 != channel2)
	conn, _ := mustConnectNSQD(tcpAddr)
	defer func() { _ = conn.Close() }()
	client2 := newClientV2(1, conn, nsqd)
	err = channel2.AddClient(client2.ID, client2)
	test.Nil(t, err)

	// let the stale timer fire
	close(resume)

	// the stale deletion attempt has now completed and must not have
	// touched the new instance
	<-deleted
	test.Equal(t, false, channel2.Exiting())
	existing, err := topic.GetExistingChannel(channelName)
	test.Nil(t, err)
	test.Equal(t, true, existing == channel2)
}

// a topic exit takes precedence over a pending ephemeral deletion of one
// of its channels: the stale timer must be a no-op
func TestEphemeralChannelStaleTimerAfterTopicDelete(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.EphemeralChannelGracePeriod = 10 * time.Millisecond

	expired := make(chan struct{})
	resume := make(chan struct{})
	deleted := make(chan struct{})
	var once sync.Once
	opts.ephemeralDeletionHook = func(c *Channel, proceed func()) {
		once.Do(func() { close(expired) })
		<-resume
		proceed()
		close(deleted)
	}

	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_ephemeral_topic_exit" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channelName := "ch#ephemeral"
	channel := topic.GetChannel(channelName)

	client := newClientV2(0, nil, nsqd)
	err := channel.AddClient(client.ID, client)
	test.Nil(t, err)
	channel.RemoveClient(client.ID)

	// wait until the grace period has expired and the deletion is
	// precisely about to trigger
	<-expired

	// the topic is deleted while the timer is in flight
	err = nsqd.DeleteExistingTopic(topicName)
	test.Nil(t, err)

	// let the stale timer fire; it must not error or resurrect anything
	close(resume)
	<-deleted

	test.Equal(t, true, channel.Exiting())
	topic.RLock()
	numChannels := len(topic.channelMap)
	topic.RUnlock()
	test.Equal(t, 0, numChannels)
}

// while an ephemeral channel is kept alive during the idle grace period
// its backlog is still bounded by the configured memory limit, it does
// not silently become a permanent queue
func TestEphemeralChannelGracePeriodBacklogBounded(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 1
	opts.EphemeralChannelGracePeriod = time.Hour
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_ephemeral_grace_backlog" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch#ephemeral")

	client := newClientV2(0, nil, nsqd)
	err := channel.AddClient(client.ID, client)
	test.Nil(t, err)
	channel.RemoveClient(client.ID)

	// the channel is now idle within its grace period; messages in
	// excess of the memory limit are dropped (ephemeral channels have
	// no persistent backend)
	for i := 0; i < 3; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		err = channel.PutMessage(msg)
		test.Nil(t, err)
	}
	test.Equal(t, int64(1), channel.Depth())
	test.Equal(t, false, channel.Exiting())
}
