package session

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/log"
	logrus "github.com/sirupsen/logrus"
)

func TestStagePublicationAvailabilityAndImmutableCopy(t *testing.T) {
	var absent *stageHub
	if out := absent.readPublication(); out.Availability != "inactive" || out.Snapshot != nil || out.Sequence != nil || out.AgeMS != nil {
		t.Fatal("inactive must be unknown, not an empty observation")
	}
	h := testStageHub(t)
	if out := h.readPublication(); out.Availability != "awaiting_sample" || out.EvidenceEpoch == nil || out.Sequence != nil {
		t.Fatal("first sample not available yet")
	}
	e := h.attach(nil)
	defer e.close()
	b := h.collect(time.Now())
	h.publish(b)
	a := h.readPublication()
	if a.Availability != "ready" || a.Sequence == nil || *a.Sequence != 1 || a.AgeMS == nil || *a.AgeMS < 0 {
		t.Fatal("publication metadata")
	}
	original := string(a.Snapshot)
	a.Snapshot[0] = '!'
	if string(h.readPublication().Snapshot) != original {
		t.Fatal("reader modified a shared publication")
	}
	// Reading, even repeatedly, neither collects nor consumes a new sequence.
	for i := 0; i < 100; i++ {
		if *h.readPublication().Sequence != 1 {
			t.Fatal("poll-driven observation")
		}
	}
	h.publish(h.collect(time.Now()))
	if *h.readPublication().Sequence != 2 {
		t.Fatal("sequence did not advance on collection")
	}
}

func TestStagePublicationLimitAndFailedExportAreExplicit(t *testing.T) {
	h := testStageHub(t)
	b := h.collect(time.Now())
	b.Sessions = []stageSnapshot{{Session: strings.Repeat("f", StageEvidenceMaxResponseBytes)}}
	b.SlowEvents = []stageSlowEvent{{Stage: "pipe_write"}}
	h.publish(b)
	out := h.readPublication()
	var decoded stageBatch
	if err := json.Unmarshal(out.Snapshot, &decoded); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(out)
	if err != nil || len(raw) > StageEvidenceMaxResponseBytes || !decoded.PayloadLimited || decoded.OmittedSessions != 1 || decoded.OmittedSlow != 1 || decoded.Sessions != nil || decoded.SlowEvents != nil {
		t.Fatal("over-limit publication must be an explicit bounded summary")
	}
	b = h.collect(time.Now())
	b.ExportAgeMS = math.NaN()
	h.publish(b)
	out = h.readPublication()
	if out.FailedExports != 1 || *out.Sequence != 1 {
		t.Fatal("encoding failure silently replaced previous sample")
	}
}

func TestStagePublicationConcurrentReadersAndOverwrite(t *testing.T) {
	h := testStageHub(t)
	e := h.attach(nil)
	defer e.close()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				out := h.readPublication()
				if out.Snapshot != nil && !json.Valid(out.Snapshot) {
					t.Error("torn snapshot")
				}
			}
		}()
	}
	for i := 0; i < 100; i++ {
		h.publish(h.collect(time.Now()))
	}
	wg.Wait()
	if *h.readPublication().Sequence != 100 {
		t.Fatal("latest publication missing")
	}
}

func TestStagePublicationBypassesBlockedSharedLogSubscriber(t *testing.T) {
	sub := log.Subscribe()
	finished := make(chan struct{})
	go func() {
		for i := 0; i < cap(sub)+3; i++ {
			log.Debugln("synthetic blocked-subscriber fixture")
		}
		close(finished)
	}()
	defer func() {
		// Drain before unsubscribe: Observable holds its mutex while Emit blocks.
		go func() {
			for range sub {
			}
		}()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("fixture logger failed to drain")
		}
		log.UnSubscribe(sub)
	}()
	deadline := time.Now().Add(time.Second)
	for len(sub) < cap(sub) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(sub) != cap(sub) {
		t.Fatal("fixture subscriber did not fill")
	}
	select {
	case <-finished:
		t.Fatal("native logger did not block on the full subscriber")
	default:
	}
	h := testStageHub(t)
	e := h.attach(nil)
	defer e.close()
	done := make(chan struct{})
	go func() {
		h.publish(h.collect(time.Now()))
		out := h.readPublication()
		if out.Availability != "ready" {
			t.Error("publication missing")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stage publication depends on shared log subscriber")
	}
}

func TestStagePublicationConsumerFixture(t *testing.T) {
	h := testStageHub(t)
	cookie := "1234"
	e := h.attach(&cookie)
	defer e.close()
	op := e.beginWrite(cmdFIN, 7)
	op.started = time.Now().Add(-150 * time.Millisecond)
	e.acquire(&op)
	e.finishWrite(&op, nil)
	h.publish(h.collect(time.Now()))
	raw, err := json.Marshal(h.readPublication())
	if err != nil || len(raw) > StageEvidenceMaxResponseBytes {
		t.Fatal("consumer fixture serialization")
	}
	// Optional private artifact for the Python contract test. It contains only
	// a synthetic socket cookie and the test hub's random token, never a peer.
	if path := os.Getenv("PCS_STAGE_FIXTURE_OUT"); path != "" {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

type stageBlockedLogSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *stageBlockedLogSink) Write(b []byte) (int, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return len(b), nil
}

func TestStagePublicationBypassesBlockedSharedLogSink(t *testing.T) {
	sink := &stageBlockedLogSink{entered: make(chan struct{}), release: make(chan struct{})}
	original := logrus.StandardLogger().Out
	logrus.SetOutput(sink)
	done := make(chan struct{})
	go func() { log.Warnln("synthetic blocked-sink fixture"); close(done) }()
	defer func() {
		close(sink.release)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("fixture log sink failed to drain")
		}
		logrus.SetOutput(original)
	}()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("fixture did not hold shared sink")
	}
	h := testStageHub(t)
	e := h.attach(nil)
	defer e.close()
	published := make(chan struct{})
	go func() { h.publish(h.collect(time.Now())); close(published) }()
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("stage publisher depends on shared sink")
	}
	if h.readPublication().Availability != "ready" {
		t.Fatal("publication missing while native log sink is blocked")
	}
}
