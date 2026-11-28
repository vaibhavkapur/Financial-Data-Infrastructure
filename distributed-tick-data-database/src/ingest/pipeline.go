package ingest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"distributed-tick-data-database/src/config"
	"distributed-tick-data-database/src/domain"
	"distributed-tick-data-database/src/storage"
)

// BackpressureError is returned explicitly when the bounded ingestion queue is
// saturated and cannot accept a batch within the configured timeout.
type BackpressureError struct {
	Code           string `json:"code"`
	QueueDepth     int    `json:"queue_depth"`
	QueueCapacity  int    `json:"queue_capacity"`
	RetryAfterMS   int    `json:"retry_after_ms"`
	Message        string `json:"message"`
}

func (e *BackpressureError) Error() string {
	return fmt.Sprintf("%s (depth=%d/%d, retry_after_ms=%d): %s",
		e.Code, e.QueueDepth, e.QueueCapacity, e.RetryAfterMS, e.Message)
}

type BatchCommitter interface {
	IngestBatch(batchID string, rawTicks []domain.RawTick, ingestTimeNS int64) (domain.BatchResult, error)
}

type queuedBatch struct {
	batchID      string
	ticks        []domain.RawTick
	ingestTimeNS int64
	respCh       chan batchCommitReply
}

type batchCommitReply struct {
	result domain.BatchResult
	err    error
}

type PipelineMetrics struct {
	SubmittedBatches     uint64 `json:"submitted_batches"`
	CommittedBatches     uint64 `json:"committed_batches"`
	BackpressureRejects  uint64 `json:"backpressure_rejects"`
	AcceptedEvents       uint64 `json:"accepted_events"`
	DuplicateEvents      uint64 `json:"duplicate_events"`
	RejectedEvents       uint64 `json:"rejected_events"`
	LateAcceptedEvents   uint64 `json:"late_accepted_events"`
	CurrentQueueDepth    int    `json:"current_queue_depth"`
	QueueCapacityBatches int    `json:"queue_capacity_batches"`
}

// Pipeline provides bounded-queue batched ingestion with explicit backpressure.
// It never silently drops an accepted tick: either a batch is durably committed
// and acknowledged, or an explicit BackpressureError / commit error is returned.
type Pipeline struct {
	cfg       config.IngestConfig
	committer BatchCommitter
	queue     chan queuedBatch
	stopCh    chan struct{}
	wg        sync.WaitGroup
	closed    atomic.Bool

	// Optional artificial delay per commit for deterministic backpressure tests.
	commitDelay time.Duration

	submittedBatches    atomic.Uint64
	committedBatches    atomic.Uint64
	backpressureRejects atomic.Uint64
	acceptedEvents      atomic.Uint64
	duplicateEvents     atomic.Uint64
	rejectedEvents      atomic.Uint64
	lateAcceptedEvents  atomic.Uint64
}

func NewPipeline(cfg config.IngestConfig, engine *storage.Engine, workers int) *Pipeline {
	return NewPipelineWithCommitter(cfg, engine, workers, 0)
}

func NewPipelineWithCommitter(cfg config.IngestConfig, committer BatchCommitter, workers int, commitDelay time.Duration) *Pipeline {
	if workers < 1 {
		workers = 1
	}
	p := &Pipeline{
		cfg:         cfg,
		committer:   committer,
		queue:       make(chan queuedBatch, cfg.QueueCapacityBatches),
		stopCh:      make(chan struct{}),
		commitDelay: commitDelay,
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *Pipeline) worker() {
	defer p.wg.Done()
	for {
		select {
		case item, ok := <-p.queue:
			if !ok {
				return
			}
			if p.commitDelay > 0 {
				time.Sleep(p.commitDelay)
			}
			res, err := p.committer.IngestBatch(item.batchID, item.ticks, item.ingestTimeNS)
			if err == nil {
				p.committedBatches.Add(1)
				p.acceptedEvents.Add(uint64(res.AcceptedCount))
				p.duplicateEvents.Add(uint64(res.DuplicateCount))
				p.rejectedEvents.Add(uint64(res.RejectedCount))
				p.lateAcceptedEvents.Add(uint64(res.LateAcceptedCount))
			}
			item.respCh <- batchCommitReply{result: res, err: err}
		case <-p.stopCh:
			// Drain remaining queued items so no acknowledged/in-flight batch is abandoned.
			for {
				select {
				case item, ok := <-p.queue:
					if !ok {
						return
					}
					res, err := p.committer.IngestBatch(item.batchID, item.ticks, item.ingestTimeNS)
					if err == nil {
						p.committedBatches.Add(1)
						p.acceptedEvents.Add(uint64(res.AcceptedCount))
						p.duplicateEvents.Add(uint64(res.DuplicateCount))
						p.rejectedEvents.Add(uint64(res.RejectedCount))
						p.lateAcceptedEvents.Add(uint64(res.LateAcceptedCount))
					}
					item.respCh <- batchCommitReply{result: res, err: err}
				default:
					return
				}
			}
		}
	}
}

// Submit enqueues a batch subject to bounded queue capacity and backpressure
// timeout, then waits for durable WAL commit acknowledgment.
func (p *Pipeline) Submit(ctx context.Context, batchID string, rawTicks []domain.RawTick, ingestTimeNS int64) (domain.BatchResult, error) {
	if p.closed.Load() {
		return domain.BatchResult{}, fmt.Errorf("ERR_PIPELINE_CLOSED: ingestion pipeline is shut down")
	}
	if len(rawTicks) > p.cfg.MaxBatchSize {
		return domain.BatchResult{}, &domain.DomainError{
			Code:    "ERR_BATCH_TOO_LARGE",
			Field:   "batch",
			Message: fmt.Sprintf("batch size %d exceeds max_batch_size %d", len(rawTicks), p.cfg.MaxBatchSize),
		}
	}
	p.submittedBatches.Add(1)
	if ingestTimeNS == 0 {
		ingestTimeNS = time.Now().UTC().UnixNano()
	}

	item := queuedBatch{
		batchID:      batchID,
		ticks:        rawTicks,
		ingestTimeNS: ingestTimeNS,
		respCh:       make(chan batchCommitReply, 1),
	}

	timer := time.NewTimer(time.Duration(p.cfg.BackpressureTimeoutMS) * time.Millisecond)
	defer timer.Stop()

	select {
	case p.queue <- item:
	case <-timer.C:
		p.backpressureRejects.Add(1)
		return domain.BatchResult{}, &BackpressureError{
			Code:          "ERR_BACKPRESSURE_QUEUE_FULL",
			QueueDepth:    len(p.queue),
			QueueCapacity: cap(p.queue),
			RetryAfterMS:  p.cfg.BackpressureTimeoutMS,
			Message:       "bounded ingestion queue saturated; retry with backoff",
		}
	case <-ctx.Done():
		p.backpressureRejects.Add(1)
		return domain.BatchResult{}, &BackpressureError{
			Code:          "ERR_BACKPRESSURE_CONTEXT_CANCELLED",
			QueueDepth:    len(p.queue),
			QueueCapacity: cap(p.queue),
			RetryAfterMS:  p.cfg.BackpressureTimeoutMS,
			Message:       ctx.Err().Error(),
		}
	}

	select {
	case rep := <-item.respCh:
		return rep.result, rep.err
	case <-ctx.Done():
		return domain.BatchResult{}, ctx.Err()
	}
}

func (p *Pipeline) Metrics() PipelineMetrics {
	return PipelineMetrics{
		SubmittedBatches:     p.submittedBatches.Load(),
		CommittedBatches:     p.committedBatches.Load(),
		BackpressureRejects:  p.backpressureRejects.Load(),
		AcceptedEvents:       p.acceptedEvents.Load(),
		DuplicateEvents:      p.duplicateEvents.Load(),
		RejectedEvents:       p.rejectedEvents.Load(),
		LateAcceptedEvents:   p.lateAcceptedEvents.Load(),
		CurrentQueueDepth:    len(p.queue),
		QueueCapacityBatches: cap(p.queue),
	}
}

func (p *Pipeline) Close() {
	if p.closed.CompareAndSwap(false, true) {
		close(p.stopCh)
		p.wg.Wait()
	}
}
