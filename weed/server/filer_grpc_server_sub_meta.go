package weed_server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

const (
	// metadataGapSettledHorizon bounds how recent a gap may be before an
	// aggregated-stream subscriber may skip past it. The aggregated ring has no
	// flush watermark of its own (persistence happens on each source filer), so
	// wall-clock age is the only local proxy: a window younger than a flush
	// interval plus clock-skew margin may still be unflushed. A flush stalled
	// beyond the horizon can still lose the skipped window — but the previous
	// unconditional skip lost it in every case. Local subscriptions gate on the
	// real flush watermark instead (resolveLocalGapResume).
	metadataGapSettledHorizon = 2 * filer.LogFlushInterval

	// unflushedGapRetryInterval caps the wait of a subscriber parked on a recent
	// (possibly-unflushed) gap, in case the flush notification is missed.
	unflushedGapRetryInterval = 2 * time.Second

	// MaxUnsyncedEvents send empty notification with timestamp when certain amount of events have been filtered
	MaxUnsyncedEvents = 1e3

	// idleHeartbeatInterval bounds how often a caught-up subscriber that asked
	// for idle heartbeats is reminded that the source is alive and has nothing
	// newer. It keeps freshness signals such as filer.sync's sync_offset metric
	// from looking stuck during read-only periods on the source.
	idleHeartbeatInterval = 5 * time.Second
)

// metadataStreamSender is satisfied by both gRPC stream types and pipelinedSender.
type metadataStreamSender interface {
	Send(*filer_pb.SubscribeMetadataResponse) error
}

const (
	// batchBehindThreshold: when an event's timestamp is older than this
	// relative to wall clock, the sender switches to batch mode for throughput.
	// When events are closer to current time, they are sent one-by-one for
	// low latency.
	batchBehindThreshold = 2 * time.Minute
	maxBatchSize         = 256
)

// pipelinedSender decouples event reading from gRPC delivery by buffering
// messages in a channel. A dedicated goroutine handles stream.Send(), allowing
// the reader to continue reading ahead without waiting for the client to
// acknowledge each event.
//
// When the client declares support for batching AND events are far behind
// current time (backlog catch-up), multiple events are packed into a single
// stream.Send() using the Events field. Otherwise events are sent one-by-one.
type pipelinedSender struct {
	sendCh   chan *filer_pb.SubscribeMetadataResponse
	errCh    chan error
	done     chan struct{}
	canBatch bool // true only if client set ClientSupportsBatching
}

func newPipelinedSender(stream metadataStreamSender, bufSize int, clientSupportsBatching bool) *pipelinedSender {
	s := &pipelinedSender{
		sendCh:   make(chan *filer_pb.SubscribeMetadataResponse, bufSize),
		errCh:    make(chan error, 1),
		done:     make(chan struct{}),
		canBatch: clientSupportsBatching,
	}
	go s.sendLoop(stream)
	return s
}

func (s *pipelinedSender) sendLoop(stream metadataStreamSender) {
	defer close(s.done)
	for msg := range s.sendCh {
		shouldBatch := s.canBatch && time.Now().UnixNano()-msg.TsNs > int64(batchBehindThreshold)

		if !shouldBatch {
			// Real-time: send immediately for low latency
			if err := stream.Send(msg); err != nil {
				s.reportErr(err)
				return
			}
			continue
		}

		// Backlog: batch multiple events into one Send for throughput.
		// The first event goes in the top-level fields; additional events
		// go in the Events slice. Old clients ignore the Events field.
		batch := make([]*filer_pb.SubscribeMetadataResponse, 0, maxBatchSize)
		batch = append(batch, msg)
	drain:
		for len(batch) < maxBatchSize {
			select {
			case next, ok := <-s.sendCh:
				if !ok {
					break drain
				}
				batch = append(batch, next)
			default:
				break drain
			}
		}

		var toSend *filer_pb.SubscribeMetadataResponse
		if len(batch) == 1 {
			toSend = batch[0]
		} else {
			// Pack batch: first event is the envelope, rest go in Events
			toSend = batch[0]
			toSend.Events = batch[1:]
		}
		if err := stream.Send(toSend); err != nil {
			s.reportErr(err)
			return
		}
		if toSend.Events != nil {
			toSend.Events = nil
		}
	}
}

func (s *pipelinedSender) reportErr(err error) {
	select {
	case s.errCh <- err:
	default:
	}
	// Don't drain sendCh here — Send() detects the exit via <-s.done
	// and the deferred close(s.done) in sendLoop will fire after this returns.
}

func (s *pipelinedSender) Send(msg *filer_pb.SubscribeMetadataResponse) error {
	select {
	case s.sendCh <- msg:
		return nil
	case err := <-s.errCh:
		return err
	case <-s.done:
		// Sender goroutine exited (stream error or shutdown).
		select {
		case err := <-s.errCh:
			return err
		default:
			return fmt.Errorf("pipelined sender closed")
		}
	}
}

func (s *pipelinedSender) Close() error {
	close(s.sendCh)
	<-s.done
	select {
	case err := <-s.errCh:
		return err
	default:
		return nil
	}
}

// resolveDiskGapResume decides whether an aggregated-stream subscriber whose
// in-memory read returned ResumeFromDiskError and whose disk read found nothing
// may skip forward. The target is the earliest in-memory timestamp, capped
// strictly below nowTsNs-settledHorizon (persisted reads exclude ts <= cursor,
// so the boundary itself stays protected). For a recent gap the cap lands
// at/before the current position and it returns false — the caller must wait
// for the flush and re-read disk. Callers should pace capped advances: the
// horizon slides with the wall clock, so immediate re-probing would spin.
func resolveDiskGapResume(currentTsNs, earliestMemTsNs, nowTsNs int64, settledHorizon time.Duration) (advanceToTsNs int64, advance bool) {
	// No in-memory data (zero time → negative UnixNano), or memory not ahead of us.
	if earliestMemTsNs <= 0 || earliestMemTsNs <= currentTsNs {
		return 0, false
	}
	// Positions are exclusive (readers deliver ts > position): resume just below
	// earliest so the earliest in-memory entry itself is still delivered.
	target := min(earliestMemTsNs-1, nowTsNs-int64(settledHorizon)-1)
	// A recent gap collapses to target <= currentTsNs → do not advance.
	if target <= currentTsNs {
		return 0, false
	}
	return target, true
}

// errHeldByPeerWatermark aborts a read when the next entry lies beyond the
// aggregated delivery hold point (see resolveAggReadHoldTsNs). The caller
// rewinds to just below the held entry, waits, and re-reads — the re-read
// re-lists persisted log files, which is what picks up a source filer's file
// that landed after the previous pass.
var errHeldByPeerWatermark = errors.New("held by aggregated peer watermark")

// resolveAggReadHoldTsNs bounds how far an aggregated subscriber may read.
// Sources merge into the aggregated stream at independent paces: per-filer
// persisted log files land whenever that filer's flush completes, and a peer
// recovering from a stall re-inserts its backlog with original (older)
// timestamps. A scalar cursor that advances past a timestamp T before every
// source has provably delivered up to T silently loses whatever arrives late.
// The hold point is therefore the peers' delivery low-watermark, relaxed by
// the settled horizon as a liveness escape: a peer stalled for longer than
// the horizon no longer holds delivery back (bounded staleness, same
// trade-off as resolveDiskGapResume — and any loss this allows was
// unconditional before the watermark existed).
func resolveAggReadHoldTsNs(peerLowWatermarkTsNs, nowTsNs int64, settledHorizon time.Duration) int64 {
	horizonTsNs := nowTsNs - int64(settledHorizon)
	if peerLowWatermarkTsNs > horizonTsNs {
		return peerLowWatermarkTsNs
	}
	return horizonTsNs
}

// previousMinuteEndTsNs returns the last nanosecond of the minute before the
// one containing tsNs. Persisted log files are named per minute, so this is
// the newest ref-listing bound that cannot include a file whose minute window
// crosses tsNs.
func previousMinuteEndTsNs(tsNs int64) int64 {
	return tsNs - tsNs%int64(time.Minute) - 1
}

// resolveLocalGapResume decides whether a local subscriber may skip a gap its
// disk read found empty. flushedTsNs is the buffer's flush watermark observed
// before that read: once it has passed the earliest in-memory timestamp, every
// event in the gap would have been on disk when the read ran, so the miss
// proves the gap empty — no wall-clock assumptions needed.
func resolveLocalGapResume(currentTsNs, earliestMemTsNs, flushedTsNs int64) (advanceToTsNs int64, advance bool) {
	// No in-memory data (zero time → negative UnixNano), or memory not ahead of us.
	if earliestMemTsNs <= 0 || earliestMemTsNs <= currentTsNs {
		return 0, false
	}
	// The gap may still hold unflushed events.
	if flushedTsNs < earliestMemTsNs {
		return 0, false
	}
	// Positions are exclusive (readers deliver ts > position): resume just below
	// earliest so the earliest in-memory entry itself is still delivered.
	target := earliestMemTsNs - 1
	if target <= currentTsNs {
		return 0, false
	}
	return target, true
}

func (fs *FilerServer) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {
	if fs.filer.MetaAggregator == nil || !fs.filer.MetaAggregator.HasRemotePeers() {
		return fs.SubscribeLocalMetadata(req, stream)
	}

	ctx := stream.Context()
	peerAddress := findClientAddress(ctx, 0)

	isReplacing, alreadyKnown, clientName := fs.addClient("", req.ClientName, peerAddress, req.PathPrefix, req.ClientId, req.ClientEpoch)
	if isReplacing {
		fs.filer.MetaAggregator.ListenersCond.Broadcast() // nudges the subscribers that are waiting
	} else if alreadyKnown {
		fs.filer.MetaAggregator.ListenersCond.Broadcast() // nudges the subscribers that are waiting
		return fmt.Errorf("duplicated subscription detected for client %s id %d", clientName, req.ClientId)
	}
	defer func() {
		glog.V(0).Infof("disconnect %v subscriber %s clientId:%d", clientName, req.PathPrefix, req.ClientId)
		fs.deleteClient("", clientName, req.ClientId, req.ClientEpoch)
		fs.filer.MetaAggregator.ListenersCond.Broadcast() // nudges the subscribers that are waiting
	}()

	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, -2)
	glog.V(0).Infof(" %v starts to subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

	sender := newPipelinedSender(stream, 1024, req.ClientSupportsBatching)
	defer sender.Close()

	// Register for instant notification when new data arrives in the aggregated log buffer.
	// Used to replace the 1127ms sleep with event-driven wake-up.
	// Key includes clientId/epoch: a replacement stream may reuse the same
	// clientName (same gRPC conn), and sharing the channel would let the old
	// stream's deferred unregister close it under the new stream.
	aggNotifyName := fmt.Sprintf("aggSubscribe:%s:%d:%d", clientName, req.ClientId, req.ClientEpoch)
	aggNotifyChan := fs.filer.MetaAggregator.MetaLogBuffer.RegisterSubscriber(aggNotifyName)
	defer fs.filer.MetaAggregator.MetaLogBuffer.UnregisterSubscriber(aggNotifyName)

	var unsyncedEvents int64
	eachEventNotificationFn := fs.eachEventNotificationFn(req, sender, clientName, &unsyncedEvents)

	// lastSeenTsNs tracks how far the subscriber has read so idle heartbeats are
	// only emitted once it is caught up to the buffer head. It is read and
	// written from this single goroutine, so no synchronization is needed.
	var lastSeenTsNs int64
	var lastHeartbeatNs int64
	baseEachLogEntryFn := eachLogEntryFn(req, sender, eachEventNotificationFn, &unsyncedEvents)
	// heldAtTsNs remembers the entry a read was held at, so the caller can
	// rewind to just below it (positions are exclusive) and re-deliver it once
	// the hold point advances.
	var heldAtTsNs int64
	// The two read paths have distinct completeness domains, each bounded by
	// its own peer watermark (with the settled horizon as liveness escape):
	//  - persisted logs are complete only up to every peer's flush watermark
	//    (a peer may still land a file, or append a chunk, below anything
	//    newer);
	//  - the aggregated ring is complete only up to every peer's delivery
	//    watermark (a peer recovering from a stall merges its backlog in late
	//    with original timestamps).
	holdDiskTsNs := func() int64 {
		return resolveAggReadHoldTsNs(fs.filer.MetaAggregator.PeerLowFlushWatermarkTsNs(), time.Now().UnixNano(), metadataGapSettledHorizon)
	}
	holdMemTsNs := func() int64 {
		return resolveAggReadHoldTsNs(fs.filer.MetaAggregator.PeerLowWatermarkTsNs(), time.Now().UnixNano(), metadataGapSettledHorizon)
	}
	// deliveredUpToTsNs tracks the newest entry actually handed to the sender,
	// so a held read can rewind to a position that skips nothing.
	var deliveredUpToTsNs int64
	guardedEachLogEntryFn := func(holdFn func() int64) log_buffer.EachLogEntryFuncType {
		return func(logEntry *filer_pb.LogEntry) (bool, error) {
			if logEntry.TsNs > holdFn() {
				heldAtTsNs = logEntry.TsNs
				return false, errHeldByPeerWatermark
			}
			lastSeenTsNs = logEntry.TsNs
			deliveredUpToTsNs = logEntry.TsNs
			return baseEachLogEntryFn(logEntry)
		}
	}
	// The disk hold point must be frozen BEFORE each pass lists the log files:
	// per-source flushes are ts-ordered, so everything at or below the flush
	// low-watermark observed before the listing is guaranteed to be in the
	// listing. Evaluating it live would let the cap rise mid-pass as flushes
	// land, admitting already-listed sources' entries past a window whose
	// late-landing files/chunks this pass cannot see.
	var diskPassHoldTsNs int64
	diskEachLogEntryFn := guardedEachLogEntryFn(func() int64 { return diskPassHoldTsNs })
	memEachLogEntryFn := guardedEachLogEntryFn(holdMemTsNs)
	// waitHeld pauses a held read until new data arrives or the retry interval
	// elapses (the hold point also advances on idle heartbeats, which do not
	// notify), then rewinds so the held entry is re-read. Returns false when
	// the subscription context ended.
	waitHeld := func() bool {
		glog.V(3).Infof("held at %v (deliveredUpTo %v, flushLow %v, deliveryLow %v) for %v",
			time.Unix(0, heldAtTsNs), time.Unix(0, deliveredUpToTsNs),
			time.Unix(0, fs.filer.MetaAggregator.PeerLowFlushWatermarkTsNs()),
			time.Unix(0, fs.filer.MetaAggregator.PeerLowWatermarkTsNs()), clientName)
		select {
		case <-aggNotifyChan:
		case <-ctx.Done():
			return false
		case <-time.After(unflushedGapRetryInterval):
		}
		return true
	}

	var processedTsNs int64
	var readPersistedLogErr error
	var readInMemoryLogErr error
	var isDone bool

	for {

		glog.V(4).Infof("read on disk %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		// Observe the flush low-watermark before the pass lists files (see
		// diskPassHoldTsNs above).
		diskPassHoldTsNs = holdDiskTsNs()

		if req.ClientSupportsMetadataChunks {
			// Cap the handed-out file refs at the last minute fully covered by
			// the hold point: the client reads refs without server-side entry
			// filtering, and a file whose minute window crosses the hold point
			// may still be missing a late-flushing source's events.
			refsStopTsNs := previousMinuteEndTsNs(diskPassHoldTsNs)
			if req.UntilNs != 0 && req.UntilNs < refsStopTsNs {
				refsStopTsNs = req.UntilNs
			}
			if refsStopTsNs > lastReadTime.Time.UnixNano() {
				processedTsNs, isDone, readPersistedLogErr = fs.sendLogFileRefs(ctx, stream, lastReadTime, refsStopTsNs)
			} else {
				processedTsNs, isDone, readPersistedLogErr = 0, false, nil
			}
		} else {
			processedTsNs, isDone, readPersistedLogErr = fs.filer.ReadPersistedLogBuffer(ctx, lastReadTime, req.UntilNs, diskEachLogEntryFn)
		}
		if errors.Is(readPersistedLogErr, errHeldByPeerWatermark) {
			// Stay at the last delivered entry: anything between the hold point
			// and the held entry may still be landed late by another source, so
			// the cursor must not move past what was actually sent. The held
			// entry is re-read (and re-checked) by the next pass.
			if processedTsNs > 0 {
				lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
			}
			// A hold is not a gap: clear any stale ResumeFromDiskError so the
			// next pass's disk-miss handling cannot skip past the held entry.
			readInMemoryLogErr = nil
			if !waitHeld() {
				return nil
			}
			continue
		}
		if readPersistedLogErr != nil {
			return fmt.Errorf("reading from persisted logs: %w", readPersistedLogErr)
		}
		if isDone {
			return nil
		}

		glog.V(4).Infof("processed to %v: %v", clientName, processedTsNs)
		if processedTsNs != 0 {
			lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
		} else {
			// No data found on disk
			// Check if we previously got ResumeFromDiskError from memory, meaning we're in a gap
			if errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError) {
				// Disk found nothing for the gap. Only skip past a window proven
				// settled; a recent gap may hold unflushed events. See resolveDiskGapResume.
				earliestTime := fs.filer.MetaAggregator.MetaLogBuffer.GetEarliestTime()
				if advanceToTsNs, advance := resolveDiskGapResume(lastReadTime.Time.UnixNano(), earliestTime.UnixNano(), time.Now().UnixNano(), metadataGapSettledHorizon); advance {
					if advanceToTsNs < earliestTime.UnixNano()-1 {
						// Loud on purpose: the aggregated path has no flush
						// watermark, so a skip capped by the wall-clock horizon
						// may pass events a stalled flush lands later. Operators
						// should see a flush stall outlasting the horizon.
						glog.Warningf("metadata gap: flush older than %v stalled? skipping from %v toward %v (earliest memory %v) for %v",
							metadataGapSettledHorizon, lastReadTime.Time, time.Unix(0, advanceToTsNs), earliestTime, clientName)
					} else {
						glog.V(3).Infof("gap detected: skipping from %v to settled position %v (earliest memory %v) for %v",
							lastReadTime.Time, time.Unix(0, advanceToTsNs), earliestTime, clientName)
					}
					lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, -2)
					if advanceToTsNs < earliestTime.UnixNano()-1 {
						// Capped mid-gap: stay on the disk-probe path (the rest of
						// the window may still be unflushed) and pace the next
						// probe — the horizon slides with the wall clock, so
						// immediate re-probing would spin.
						select {
						case <-aggNotifyChan:
						case <-ctx.Done():
							return nil
						case <-time.After(unflushedGapRetryInterval):
						}
						continue
					}
					readInMemoryLogErr = nil // Reached the in-memory window: resume from memory
				} else {
					// Recent (possibly-unflushed) gap — or an aggregated buffer
					// with no readable entries (zero earliestTime): wait for
					// flush/new data, then re-read disk. Falling through to the
					// in-memory read here would return ResumeFromDiskError
					// again immediately and spin without waiting.
					glog.V(3).Infof("unflushed gap at %v (earliest memory %v) for %v: waiting for flush before advancing",
						lastReadTime.Time, earliestTime, clientName)
					select {
					case <-aggNotifyChan:
					case <-ctx.Done():
						return nil
					case <-time.After(unflushedGapRetryInterval):
					}
					continue
				}
			} else {
				// First pass or no ResumeFromDiskError yet - check the next day for logs
				nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
				// The day jump advances the cursor without delivering; past the
				// hold point a late-flushing source could still land a file in
				// the skipped range, so stay put until the hold point covers it.
				if nextDayTs <= diskPassHoldTsNs {
					position := log_buffer.NewMessagePosition(nextDayTs, -2)
					found, err := fs.filer.HasPersistedLogFiles(position)
					if err != nil {
						return fmt.Errorf("checking persisted log files: %w", err)
					}
					if found {
						lastReadTime = position
					}
				}
			}
		}

		if lastReadTime.Time.UnixNano() > deliveredUpToTsNs {
			deliveredUpToTsNs = lastReadTime.Time.UnixNano()
		}

		glog.V(4).Infof("read in memory %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		// Reader name includes clientId/epoch for the same reason as aggNotifyName:
		// LoopProcessLogData registers it as a subscriber key internally.
		lastReadTime, isDone, readInMemoryLogErr = fs.filer.MetaAggregator.MetaLogBuffer.LoopProcessLogData(fmt.Sprintf("aggMeta:%s:%d:%d", clientName, req.ClientId, req.ClientEpoch), lastReadTime, req.UntilNs, func() bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			if !fs.hasClient(req.ClientId, req.ClientEpoch) {
				return false
			}
			lastHeartbeatNs = fs.maybeSendIdleHeartbeat(req, sender, fs.filer.MetaAggregator.MetaLogBuffer, lastReadTime.Time.UnixNano(), lastSeenTsNs, lastHeartbeatNs)
			return true
		}, memEachLogEntryFn)
		if readInMemoryLogErr != nil {
			if errors.Is(readInMemoryLogErr, errHeldByPeerWatermark) {
				// The in-memory read cursor already advanced onto the held
				// entry; rewind to the last entry actually delivered so nothing
				// between the hold point and the held entry can be skipped.
				lastReadTime = log_buffer.NewMessagePosition(deliveredUpToTsNs, -2)
				readInMemoryLogErr = nil
				if !waitHeld() {
					return nil
				}
				continue
			}
			if errors.Is(readInMemoryLogErr, log_buffer.ResumeFromDiskError) {
				// Memory says data is too old - will read from disk on next iteration
				// But if disk also has no data (gap in history), we'll skip forward
				continue
			}
			glog.Errorf("processed to %v: %v", lastReadTime, readInMemoryLogErr)
			if !errors.Is(readInMemoryLogErr, log_buffer.ResumeError) {
				break
			}
		}
		if isDone {
			return nil
		}
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			glog.V(0).Infof("client %v is closed", clientName)
			return nil
		}

		// Wait for new data (event-driven instead of 1127ms polling).
		// Drain any stale notification first to avoid a spurious wake-up.
		select {
		case <-aggNotifyChan:
		default:
		}
		select {
		case <-aggNotifyChan:
		case <-ctx.Done():
			return nil
		}
	}

	return readInMemoryLogErr

}

func (fs *FilerServer) SubscribeLocalMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeLocalMetadataServer) error {

	ctx := stream.Context()
	peerAddress := findClientAddress(ctx, 0)

	// use negative client id to differentiate from addClient()/deleteClient() used in SubscribeMetadata()
	req.ClientId = -req.ClientId

	isReplacing, alreadyKnown, clientName := fs.addClient("local", req.ClientName, peerAddress, req.PathPrefix, req.ClientId, req.ClientEpoch)
	if isReplacing {
		fs.listenersCond.Broadcast() // nudges the subscribers that are waiting
	} else if alreadyKnown {
		return fmt.Errorf("duplicated local subscription detected for client %s clientId:%d", clientName, req.ClientId)
	}
	defer func() {
		glog.V(0).Infof("disconnect %v local subscriber %s clientId:%d", clientName, req.PathPrefix, req.ClientId)
		fs.deleteClient("local", clientName, req.ClientId, req.ClientEpoch)
		fs.listenersCond.Broadcast() // nudges the subscribers that are waiting
	}()

	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, -2)
	glog.V(0).Infof(" + %v local subscribe %s from %+v clientId:%d", clientName, req.PathPrefix, lastReadTime, req.ClientId)

	sender := newPipelinedSender(stream, 1024, req.ClientSupportsBatching)
	defer sender.Close()

	// Bounded gap waits use the buffer's subscriber notification plus a retry
	// timer, so a flush landing between the disk read and the wait cannot
	// strand the subscriber (no lost-wakeup window). Key includes clientId/
	// epoch so a replacement stream never shares (and loses) the channel.
	localNotifyName := fmt.Sprintf("localGap:%s:%d:%d", clientName, req.ClientId, req.ClientEpoch)
	localNotifyChan := fs.filer.LocalMetaLogBuffer.RegisterSubscriber(localNotifyName)
	defer fs.filer.LocalMetaLogBuffer.UnregisterSubscriber(localNotifyName)

	var unsyncedEvents int64
	eachEventNotificationFn := fs.eachEventNotificationFn(req, sender, clientName, &unsyncedEvents)

	// lastSeenTsNs tracks how far the subscriber has read so idle heartbeats are
	// only emitted once it is caught up to the buffer head. It is read and
	// written from this single goroutine, so no synchronization is needed.
	var lastSeenTsNs int64
	var lastHeartbeatNs int64
	var lastFlushReportNs int64
	baseEachLogEntryFn := eachLogEntryFn(req, sender, eachEventNotificationFn, &unsyncedEvents)
	eachLogEntryFn := func(logEntry *filer_pb.LogEntry) (bool, error) {
		lastSeenTsNs = logEntry.TsNs
		return baseEachLogEntryFn(logEntry)
	}

	var processedTsNs int64
	var readPersistedLogErr error
	var readInMemoryLogErr error
	var isDone bool
	var lastCheckedFlushTsNs int64 = -1 // Track the last flushed time we checked
	var lastDiskReadTsNs int64 = -1     // Track the last read position we used for disk read

	for {
		// Check if new data has been flushed to disk since last check, or if read position advanced
		currentFlushTsNs := fs.filer.LocalMetaLogBuffer.GetLastFlushTsNs()
		currentReadTsNs := lastReadTime.Time.UnixNano()
		// Read from disk if: first time, new flush observed, or read position advanced (draining backlog)
		shouldReadFromDisk := lastCheckedFlushTsNs == -1 ||
			currentFlushTsNs > lastCheckedFlushTsNs ||
			currentReadTsNs > lastDiskReadTsNs

		if shouldReadFromDisk {
			// Record the position we are about to read from
			lastDiskReadTsNs = currentReadTsNs
			glog.V(4).Infof("read on disk %v local subscribe %s from %+v (lastFlushed: %v)", clientName, req.PathPrefix, lastReadTime, time.Unix(0, currentFlushTsNs))
			if req.ClientSupportsMetadataChunks {
				processedTsNs, isDone, readPersistedLogErr = fs.sendLogFileRefs(ctx, stream, lastReadTime, req.UntilNs)
			} else {
				processedTsNs, isDone, readPersistedLogErr = fs.filer.ReadPersistedLogBuffer(ctx, lastReadTime, req.UntilNs, eachLogEntryFn)
			}
			if readPersistedLogErr != nil {
				glog.V(0).Infof("read on disk %v local subscribe %s from %+v: %v", clientName, req.PathPrefix, lastReadTime, readPersistedLogErr)
				return fmt.Errorf("reading from persisted logs: %w", readPersistedLogErr)
			}
			if isDone {
				return nil
			}

			// Update the last checked flushed time
			lastCheckedFlushTsNs = currentFlushTsNs

			if processedTsNs != 0 {
				lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
			} else {
				// No data found on disk
				// Check if we previously got ResumeFromDiskError from memory, meaning we're in a gap
				if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
					// The read above ran after observing the currentFlushTsNs
					// watermark and found nothing: once that watermark has passed
					// the earliest in-memory time, the gap is provably empty.
					earliestTime := fs.filer.LocalMetaLogBuffer.GetEarliestTime()
					if advanceToTsNs, advance := resolveLocalGapResume(lastReadTime.Time.UnixNano(), earliestTime.UnixNano(), currentFlushTsNs); advance {
						glog.V(3).Infof("gap detected: skipping from %v to flushed earliest memory time %v for %v",
							lastReadTime.Time, earliestTime, clientName)
						lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, -2)
						readInMemoryLogErr = nil // Clear the error since we're skipping forward
					} else if currentFlushTsNs >= earliestTime.UnixNano() && earliestTime.UnixNano()-1 == lastReadTime.Time.UnixNano() {
						// Exact-boundary corner: the gap is provably empty (the
						// flush watermark has passed the earliest in-memory
						// entry) but the exclusive resume target collapses onto
						// the current cursor, so resolveLocalGapResume cannot
						// advance. Re-arm the cursor with a positive (exclusive)
						// offset instead: ReadFromBuffer explicitly allows it at
						// the eviction watermark, so the earliest in-memory
						// entry is served from memory without waiting another
						// flush cycle.
						lastReadTime = log_buffer.NewMessagePosition(lastReadTime.Time.UnixNano(), 1)
						readInMemoryLogErr = nil
					} else {
						// The gap may hold unflushed events: wait (bounded) for
						// flush/new data, then re-read disk.
						select {
						case <-localNotifyChan:
						case <-ctx.Done():
							return nil
						case <-time.After(unflushedGapRetryInterval):
						}
						continue
					}
				} else {
					// First pass or no ResumeFromDiskError yet
					// Check the next day for logs
					nextDayTs := util.GetNextDayTsNano(lastReadTime.Time.UnixNano())
					position := log_buffer.NewMessagePosition(nextDayTs, -2)
					found, err := fs.filer.HasPersistedLogFiles(position)
					if err != nil {
						return fmt.Errorf("checking persisted log files: %w", err)
					}
					if found {
						lastReadTime = position
					}
				}
			}
		}

		glog.V(3).Infof("read in memory %v local subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		// Reader name includes clientId/epoch for the same reason as localNotifyName:
		// LoopProcessLogData registers it as a subscriber key internally.
		lastReadTime, isDone, readInMemoryLogErr = fs.filer.LocalMetaLogBuffer.LoopProcessLogData(fmt.Sprintf("localMeta:%s:%d:%d", clientName, req.ClientId, req.ClientEpoch), lastReadTime, req.UntilNs, func() bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			if !fs.hasClient(req.ClientId, req.ClientEpoch) {
				return false
			}
			lastHeartbeatNs = fs.maybeSendIdleHeartbeat(req, sender, fs.filer.LocalMetaLogBuffer, lastReadTime.Time.UnixNano(), lastSeenTsNs, lastHeartbeatNs)
			lastFlushReportNs = fs.maybeSendFlushReport(req, sender, fs.filer.LocalMetaLogBuffer, lastFlushReportNs)
			return true
		}, eachLogEntryFn)
		if readInMemoryLogErr != nil {
			if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
				// Memory buffer says the requested time is too old
				// Retry disk read if: (a) flush advanced, or (b) read position advanced (draining backlog)
				currentFlushTsNs := fs.filer.LocalMetaLogBuffer.GetLastFlushTsNs()
				currentReadTsNs := lastReadTime.Time.UnixNano()
				if currentFlushTsNs > lastCheckedFlushTsNs || currentReadTsNs > lastDiskReadTsNs {
					glog.V(0).Infof("retry disk read %v local subscribe %s (lastFlushed: %v -> %v, readTs: %v -> %v)",
						clientName, req.PathPrefix,
						time.Unix(0, lastCheckedFlushTsNs), time.Unix(0, currentFlushTsNs),
						time.Unix(0, lastDiskReadTsNs), time.Unix(0, currentReadTsNs))
					continue
				}
				// No flush or read-position progress since the last disk read: that
				// read already proved everything up to the lastCheckedFlushTsNs
				// watermark, so skip only if it covers the earliest in-memory time.
				earliestTime := fs.filer.LocalMetaLogBuffer.GetEarliestTime()
				if advanceToTsNs, advance := resolveLocalGapResume(currentReadTsNs, earliestTime.UnixNano(), lastCheckedFlushTsNs); advance {
					glog.V(3).Infof("gap detected: skipping from %v to flushed earliest memory time %v for %v",
						lastReadTime.Time, earliestTime, clientName)
					lastReadTime = log_buffer.NewMessagePosition(advanceToTsNs, -2)
					// Clear the error so the next iteration re-reads disk.
					readInMemoryLogErr = nil
					continue
				}
				// The gap may hold unflushed events: wait (bounded) for
				// flush/new data, then re-evaluate.
				select {
				case <-localNotifyChan:
				case <-ctx.Done():
					return nil
				case <-time.After(unflushedGapRetryInterval):
				}
				continue
			}
			glog.Errorf("processed to %v: %v", lastReadTime, readInMemoryLogErr)
			if readInMemoryLogErr != log_buffer.ResumeError {
				break
			}
		}
		if isDone {
			return nil
		}
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			return nil
		}
	}

	return readInMemoryLogErr

}

func eachLogEntryFn(req *filer_pb.SubscribeMetadataRequest, sender metadataStreamSender, eachEventNotificationFn func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error, filtered *int64) log_buffer.EachLogEntryFuncType {
	// A shallow scan of the path fields skips unmarshaling chunk-heavy events
	// this subscriber would filter out anyway; scan surprises fall back to the
	// full decode. Only a delivery resets the shared unsynced-events counter.
	prefilter := req.PathPrefix != "" || len(req.PathPrefixes) > 0 || len(req.Directories) > 0
	return func(logEntry *filer_pb.LogEntry) (bool, error) {
		if prefilter {
			if skeleton, ok := filer_pb.ScanMetadataEventSkeleton(logEntry.Data); ok &&
				!filer_pb.MetadataEventMatchesSubscription(skeleton, req.PathPrefix, req.PathPrefixes, req.Directories) {
				*filtered++
				if *filtered > MaxUnsyncedEvents {
					if err := sender.Send(&filer_pb.SubscribeMetadataResponse{
						EventNotification: &filer_pb.EventNotification{},
						TsNs:              skeleton.TsNs,
					}); err != nil {
						return false, err
					}
					*filtered = 0
				}
				return false, nil
			}
		}
		event := &filer_pb.SubscribeMetadataResponse{}
		// proto.Unmarshal (not UnmarshalVT) validates UTF-8 in string fields, so
		// malformed metadata is rejected here instead of reaching path filtering
		// and subscribers.
		if err := proto.Unmarshal(logEntry.Data, event); err != nil {
			glog.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %v", err)
			return false, fmt.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %w", err)
		}

		if err := eachEventNotificationFn(event.Directory, event.EventNotification, event.TsNs); err != nil {
			return false, err
		}

		return false, nil
	}
}

// maybeSendFlushReport periodically reports this buffer's flush-through
// watermark to a subscriber that opted into idle heartbeats (peer
// aggregators). Unlike idle heartbeats it is sent regardless of how far the
// subscriber has caught up: the watermark bounds other subscribers'
// persisted-log reads, so it must keep advancing while this stream is busy
// replaying a backlog. TsNs stays zero so it never advances delivery
// freshness.
func (fs *FilerServer) maybeSendFlushReport(req *filer_pb.SubscribeMetadataRequest, sender metadataStreamSender, logBuffer *log_buffer.LogBuffer, lastFlushReportNs int64) int64 {
	if !req.ClientSupportsIdleHeartbeat {
		return lastFlushReportNs
	}
	now := time.Now().UnixNano()
	if now-lastFlushReportNs < int64(idleHeartbeatInterval) {
		return lastFlushReportNs
	}
	if err := sender.Send(&filer_pb.SubscribeMetadataResponse{FlushedTsNs: logBuffer.FlushedThroughTsNs(now)}); err != nil {
		glog.V(0).Infof("=> flush report to %s: %v", req.ClientName, err)
		return lastFlushReportNs
	}
	return now
}

// maybeSendIdleHeartbeat emits an empty response carrying the current time when
// the subscriber has consumed everything up to the buffer head. The client uses
// it to advance freshness signals (e.g. filer.sync's sync_offset) without moving
// its resume checkpoint, so a restart still re-reads from the last real event.
//
// The catch-up floor is the max of two read-progress markers:
//   - readPositionTsNs: how far the read cursor has advanced. It starts at
//     SinceNs and also covers metadata-chunks mode, where persisted entries are
//     replayed as log file refs rather than through eachLogEntryFn.
//   - lastSeenTsNs: the timestamp of the most recent entry streamed in this
//     call. It advances live while reading the in-memory backlog, before the
//     read cursor returned by LoopProcessLogData has been updated.
//
// While the buffer head is past that floor the subscriber is still behind (e.g.
// replaying a backlog) and no heartbeat is sent. Returns the (possibly advanced)
// lastHeartbeatNs.
func (fs *FilerServer) maybeSendIdleHeartbeat(req *filer_pb.SubscribeMetadataRequest, sender metadataStreamSender, logBuffer *log_buffer.LogBuffer, readPositionTsNs, lastSeenTsNs, lastHeartbeatNs int64) int64 {
	if !req.ClientSupportsIdleHeartbeat {
		return lastHeartbeatNs
	}
	floorTsNs := lastSeenTsNs
	if readPositionTsNs > floorTsNs {
		floorTsNs = readPositionTsNs
	}
	if logBuffer.LastTsNs.Load() > floorTsNs {
		// the buffer holds data the subscriber has not reached yet
		return lastHeartbeatNs
	}
	now := time.Now().UnixNano()
	if now-lastHeartbeatNs < int64(idleHeartbeatInterval) {
		return lastHeartbeatNs
	}
	// Piggyback the local flush watermark so a peer aggregator can bound its
	// subscribers' persisted-log reads to flush-complete data (everything at
	// or below it is on disk on this filer).
	if err := sender.Send(&filer_pb.SubscribeMetadataResponse{TsNs: now, FlushedTsNs: logBuffer.FlushedThroughTsNs(now)}); err != nil {
		glog.V(0).Infof("=> idle heartbeat to %s: %v", req.ClientName, err)
		return lastHeartbeatNs
	}
	// A heartbeat is a send too: advance the freshness gauge so an idle but
	// healthy subscriber doesn't look stale. The gauge otherwise only moves on
	// real matching events, which never arrive on a quiet path.
	var sourceFiler string
	if fs.option != nil {
		sourceFiler = fs.option.Host.String()
	}
	stats.FilerServerLastSendTsOfSubscribeGauge.WithLabelValues(sourceFiler, req.ClientName, req.PathPrefix).Set(float64(now))
	return now
}

// sendLogFileRefs collects persisted log file chunk references and sends them
// to the client so it can read the data directly from volume servers.
// This does zero volume server I/O — it only lists filer store directory entries.
// Sends directly on the gRPC stream (bypasses pipelinedSender) because ref
// messages have TsNs=0 and must not be batched into Events by the sender.
func (fs *FilerServer) sendLogFileRefs(ctx context.Context, stream metadataStreamSender, startPosition log_buffer.MessagePosition, stopTsNs int64) (lastTsNs int64, isDone bool, err error) {
	refs, lastTsNs, err := fs.filer.CollectLogFileRefs(ctx, startPosition, stopTsNs)
	if err != nil {
		return 0, false, err
	}
	if len(refs) == 0 {
		return 0, false, nil
	}

	const maxRefsPerMessage = 64
	for i := 0; i < len(refs); i += maxRefsPerMessage {
		end := i + maxRefsPerMessage
		if end > len(refs) {
			end = len(refs)
		}
		if err := stream.Send(&filer_pb.SubscribeMetadataResponse{
			LogFileRefs: refs[i:end],
		}); err != nil {
			return lastTsNs, false, err
		}
	}
	return lastTsNs, false, nil
}

func (fs *FilerServer) eachEventNotificationFn(req *filer_pb.SubscribeMetadataRequest, sender metadataStreamSender, clientName string, filtered *int64) func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
	return func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
		defer func() {
			if *filtered > MaxUnsyncedEvents {
				if err := sender.Send(&filer_pb.SubscribeMetadataResponse{
					EventNotification: &filer_pb.EventNotification{},
					TsNs:              tsNs,
				}); err == nil {
					*filtered = 0
				}
			}
		}()

		*filtered++
		foundSelf := false
		for _, sig := range eventNotification.Signatures {
			if sig == req.Signature && req.Signature != 0 {
				return nil
			}
			if sig == fs.filer.Signature {
				foundSelf = true
			}
		}
		if !foundSelf {
			eventNotification.Signatures = append(eventNotification.Signatures, fs.filer.Signature)
		}

		// get complete path to the file or directory
		var entryName string
		if eventNotification.OldEntry != nil {
			entryName = eventNotification.OldEntry.Name
		} else if eventNotification.NewEntry != nil {
			entryName = eventNotification.NewEntry.Name
		}

		fullpath := util.Join(dirPath, entryName)

		// skip on filer internal meta logs
		if strings.HasPrefix(fullpath, filer.SystemLogDir) {
			return nil
		}

		message := &filer_pb.SubscribeMetadataResponse{
			Directory:         dirPath,
			EventNotification: eventNotification,
			TsNs:              tsNs,
		}

		if !filer_pb.MetadataEventMatchesSubscription(message, req.PathPrefix, req.PathPrefixes, req.Directories) {
			return nil
		}

		// collect timestamps for path
		stats.FilerServerLastSendTsOfSubscribeGauge.WithLabelValues(fs.option.Host.String(), req.ClientName, req.PathPrefix).Set(float64(tsNs))

		// println("sending", dirPath, entryName)
		if err := sender.Send(message); err != nil {
			glog.V(0).Infof("=> client %v: %+v", clientName, err)
			return err
		}
		*filtered = 0
		return nil
	}
}

func (fs *FilerServer) addClient(scope string, clientType string, clientAddress string, pathPrefix string, clientId int32, clientEpoch int32) (isReplacing, alreadyKnown bool, clientName string) {
	clientName = clientType + "@" + clientAddress
	glog.V(0).Infof("+ %v listener %v clientId %v clientEpoch %v", scope, clientName, clientId, clientEpoch)
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()
		epoch, found := fs.knownListeners[clientId]
		if !found || epoch < clientEpoch {
			fs.knownListeners[clientId] = clientEpoch
			isReplacing = true
			if fs.subscribers == nil {
				fs.subscribers = make(map[int32]*metadataSubscriber)
			}
			fs.subscribers[clientId] = &metadataSubscriber{
				clientName:    clientName,
				clientType:    clientType,
				address:       clientAddress,
				pathPrefix:    pathPrefix,
				clientId:      clientId,
				clientEpoch:   clientEpoch,
				connectedAtNs: time.Now().UnixNano(),
			}
		} else {
			alreadyKnown = true
		}
	}
	return
}

func (fs *FilerServer) deleteClient(scope string, clientName string, clientId int32, clientEpoch int32) {
	glog.V(0).Infof("- %v listener %v clientId %v clientEpoch %v", scope, clientName, clientId, clientEpoch)
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()
		epoch, found := fs.knownListeners[clientId]
		if found && epoch <= clientEpoch {
			delete(fs.knownListeners, clientId)
			delete(fs.subscribers, clientId)
		}
	}
}

func (fs *FilerServer) hasClient(clientId int32, clientEpoch int32) bool {
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()
		epoch, found := fs.knownListeners[clientId]
		if found && epoch <= clientEpoch {
			return true
		}
	}
	return false
}
