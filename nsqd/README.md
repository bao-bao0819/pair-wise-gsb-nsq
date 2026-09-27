## nsqd

`nsqd` is the daemon that receives, queues, and delivers messages to clients.

Read the [docs](https://nsq.io/components/nsqd.html)

## Deferred message persistence

Deferred messages (published via `DPUB` or requeued via `REQ` with a
non-zero timeout) are persisted per channel so a clean restart does not
lose their remaining delay.

On a clean shutdown each channel writes its not-yet-due deferred messages
to `<topic>:<channel>.deferred.dat` in `--data-path` (on Windows `;` is
used instead of `:`). Each record stores the message together with its
**absolute expiration timestamp** (UnixNano). On startup the file is
loaded and then deleted, so messages can never be restored twice:

- messages whose expiration passed while `nsqd` was down are made
  available for delivery immediately;
- messages not yet due are re-deferred until their *original* absolute
  expiration time — the delay is never restarted from zero;
- messages that had already transitioned from the deferred queue to the
  live queue before shutdown are flushed to the normal backend diskqueue
  (as before) and are delivered exactly once.

The on-disk format of the regular diskqueue files is unchanged, so
existing data remains fully compatible (messages persisted by older
versions simply come back as regular messages, as they always did).

### Clock changes

The persisted absolute expiration timestamp is the source of truth; the
in-memory priority queue is only a runtime view of it:

- **Clock jumps backward**: delivery happens later in wall-clock time.
  The absolute target is unchanged, so no message is delivered early and
  no delay is re-computed.
- **Clock jumps forward**: any message whose absolute expiration is now
  in the past is delivered on the next queue scan (or immediately at
  startup). No message is re-deferred.

Note that, as before, deferred state is only written on a *clean*
shutdown; an unclean kill can still lose in-memory deferred messages.
