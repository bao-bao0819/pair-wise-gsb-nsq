package nsqd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
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

func waitForEphemeralChannelDeletion(t *testing.T, topic *Topic, channelName string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := topic.GetExistingChannel(channelName)
		if err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("channel(%s) was not deleted", channelName)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// a consumer that resubscribes while the grace-period deletion is about to
// trigger must cancel the deletion; the channel (and its client) survive
func TestEphemeralChannelGracePeriodResubscribe(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 100
	opts.EphemeralChannelGracePeriod = 10 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "ephemeral_grace_resub" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch1#ephemeral")

	// synchronization point: parks the grace-period timer callback at the
	// exact moment it is about to evaluate deletion
	hookEntered := make(chan struct{})
	hookRelease := make(chan struct{})
	channel.ephemeralDeleteHook = func() {
		close(hookEntered)
		<-hookRelease
	}

	client := newClientV2(0, nil, nsqd)
	err := channel.AddClient(client.ID, client)
	test.Nil(t, err)
	channel.RemoveClient(client.ID)

	// wait until the grace period expired and deletion is about to trigger
	select {
	case <-hookEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("grace period timer did not fire")
	}

	// resubscribe exactly as deletion is about to trigger
	client2 := newClientV2(1, nil, nsqd)
	err = channel.AddClient(client2.ID, client2)
	test.Nil(t, err)
	close(hookRelease)

	// the resubscribe must have cancelled the pending deletion
	test.Equal(t, false, channel.Exiting())
	channel.RLock()
	timerCancelled := channel.ephemeralTimer == nil
	channel.RUnlock()
	test.Equal(t, true, timerCancelled)
	existingChannel, err := topic.GetExistingChannel("ch1#ephemeral")
	test.Nil(t, err)
	test.Equal(t, channel, existingChannel)

	// a subsequent idle period must still schedule (and execute) deletion,
	// i.e. the cancelled timer did not consume the one-shot deleter
	channel.ephemeralDeleteHook = nil
	channel.RemoveClient(client2.ID)
	waitForEphemeralChannelDeletion(t, topic, "ch1#ephemeral")
}

// an idle ephemeral channel is deleted once the grace period expires and,
// while waiting, its backlog stays bounded by the configured mem-queue-size
// (the grace period must not turn it into an unbounded queue)
func TestEphemeralChannelGracePeriodExpiry(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 2
	opts.EphemeralChannelGracePeriod = 10 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "ephemeral_grace_expiry" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch1#ephemeral")

	// park the deletion so the backlog can be observed deterministically
	// while the channel is inside its grace period
	hookEntered := make(chan struct{})
	hookRelease := make(chan struct{})
	channel.ephemeralDeleteHook = func() {
		close(hookEntered)
		<-hookRelease
	}

	client := newClientV2(0, nil, nsqd)
	err := channel.AddClient(client.ID, client)
	test.Nil(t, err)
	channel.RemoveClient(client.ID)

	select {
	case <-hookEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("grace period timer did not fire")
	}

	// publish more messages than mem-queue-size while the channel is idle
	// in its grace period
	for i := 0; i < 10; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("grace_period_message"))
		err := topic.PutMessage(msg)
		test.Nil(t, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for channel.Depth() < opts.MemQueueSize {
		if time.Now().After(deadline) {
			t.Fatal("messages were not routed to the idle channel")
		}
		time.Sleep(10 * time.Millisecond)
	}
	test.Equal(t, opts.MemQueueSize, channel.Depth())

	// no consumer reattached, so the deletion must proceed
	close(hookRelease)
	waitForEphemeralChannelDeletion(t, topic, "ch1#ephemeral")
}

// explicit deletion and re-creation of a same-named channel take priority
// over a pending grace-period recycle: the old timer must not delete the
// new channel instance
func TestEphemeralChannelGracePeriodStaleTimer(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 100
	opts.EphemeralChannelGracePeriod = 10 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "ephemeral_grace_stale" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch1#ephemeral")

	hookEntered := make(chan struct{})
	hookRelease := make(chan struct{})
	channel.ephemeralDeleteHook = func() {
		close(hookEntered)
		<-hookRelease
	}

	client := newClientV2(0, nil, nsqd)
	err := channel.AddClient(client.ID, client)
	test.Nil(t, err)
	channel.RemoveClient(client.ID)

	// the old channel's grace-period timer fired and is parked at the
	// deletion decision point
	select {
	case <-hookEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("grace period timer did not fire")
	}

	// explicit deletion wins over the pending recycle task
	err = topic.DeleteExistingChannel("ch1#ephemeral")
	test.Nil(t, err)

	// a new channel reuses the same name
	newChannel := topic.GetChannel("ch1#ephemeral")
	if newChannel == channel {
		t.Fatal("expected a new channel instance")
	}

	// release the old timer; it must not delete the new instance
	close(hookRelease)

	test.Equal(t, false, newChannel.Exiting())
	existingChannel, err := topic.GetExistingChannel("ch1#ephemeral")
	test.Nil(t, err)
	test.Equal(t, newChannel, existingChannel)
}
