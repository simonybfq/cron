package cron

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var cst = time.FixedZone("CST", 8*3600)

func at(y, mo, d, h, mi, s int) time.Time {
	return time.Date(y, time.Month(mo), d, h, mi, s, 0, cst)
}

// 基准时间：2026-11-09 12:00:00 周一
var base = at(2026, 11, 9, 12, 0, 0)

func TestNext(t *testing.T) {
	cases := []struct {
		expr string
		now  time.Time
		want time.Time
	}{
		// README 中的例子
		{"0 * * * * ?", base, at(2026, 11, 9, 12, 1, 0)},
		{"0 0 * * * ?", base, at(2026, 11, 9, 13, 0, 0)},
		{"0 0 10 * * ?", base, at(2026, 11, 10, 10, 0, 0)},
		{"0 * 14 * * ?", base, at(2026, 11, 9, 14, 0, 0)},
		{"0 30 9 1 * ?", base, at(2026, 12, 1, 9, 30, 0)},
		{"0 15 10 15 * ?", base, at(2026, 11, 15, 10, 15, 0)},
		{"*/5 * * * * ?", base, at(2026, 11, 9, 12, 0, 5)},
		{"0 */1 * * * ?", base, at(2026, 11, 9, 12, 1, 0)},
		{"0 0 5-15 * * ?", base, at(2026, 11, 9, 13, 0, 0)},
		{"0 0-5 14 * * ?", base, at(2026, 11, 9, 14, 0, 0)},
		{"0 0/5 14,18 * * ?", at(2026, 11, 9, 14, 57, 0), at(2026, 11, 9, 18, 0, 0)},
		{"0 0/30 9-17 * * ?", base, at(2026, 11, 9, 12, 30, 0)},
		{"0 0 10,14,16 * * ?", base, at(2026, 11, 9, 14, 0, 0)},
		{"0 0 12 ? * WED", base, at(2026, 11, 11, 12, 0, 0)},
		{"0 0 17 ? * TUE,THU,SAT", base, at(2026, 11, 10, 17, 0, 0)},
		{"0 10,30 14 ? 3 WED", base, at(2027, 3, 3, 14, 10, 0)},
		{"0 15 10 ? * MON-FRI", base, at(2026, 11, 10, 10, 15, 0)},
		{"0 15 10 ? * 6L", base, at(2026, 11, 28, 10, 15, 0)},
		{"0 0 0 ? 5 0#2", base, at(2027, 5, 9, 0, 0, 0)},
		{"0 0 23 L * ?", base, at(2026, 11, 30, 23, 0, 0)},
		{"0 15 10 LW * ?", base, at(2026, 11, 30, 10, 15, 0)},

		// 评审中复现的错误场景
		{"0 0 10 15 * ?", base, at(2026, 11, 15, 10, 0, 0)},
		{"0 0 0 31 * ?", base, at(2026, 12, 31, 0, 0, 0)},
		{"0 0 10 * 3,6 ?", at(2026, 4, 10, 12, 0, 0), at(2026, 6, 1, 10, 0, 0)},
		{"0 0 9 ? * MON,FRI", at(2026, 6, 30, 12, 0, 0), at(2026, 7, 3, 9, 0, 0)},
		{"0 0 0 1W * ?", at(2026, 7, 10, 0, 0, 0), at(2026, 8, 3, 0, 0, 0)}, // 8/1 周六 -> 8/3 周一
		{"0 0 12 15W * ?", base, at(2026, 11, 16, 12, 0, 0)},                // 11/15 周日 -> 11/16 周一
		{"0 0 12 13W * ?", base, at(2026, 11, 13, 12, 0, 0)},                // 本身是工作日
		{"0 0 12 ? * 0#2", base, at(2026, 12, 13, 12, 0, 0)},
		{"0 0 0 ? * 1#5", base, at(2026, 11, 30, 0, 0, 0)},
		{"0 0 0 29 2 ?", base, at(2028, 2, 29, 0, 0, 0)},

		// 进位与边界
		{"* * * * * ?", base.Add(500 * time.Millisecond), at(2026, 11, 9, 12, 0, 1)},
		{"0 0 10 * * ?", at(2026, 11, 30, 11, 0, 0), at(2026, 12, 1, 10, 0, 0)},
		{"0 0 0 1 1 ?", base, at(2027, 1, 1, 0, 0, 0)},
		{"59 59 23 31 12 ?", at(2026, 12, 31, 23, 59, 59), at(2027, 12, 31, 23, 59, 59)},
		{"0 0 1,3-5,20/2 * * ?", base, at(2026, 11, 9, 20, 0, 0)},
		{"0 0 12 * JAN-MAR ?", base, at(2027, 1, 1, 12, 0, 0)},
		{"0 0 12 ? * sun", base, at(2026, 11, 15, 12, 0, 0)},
		{"0  0   10 * * ?", base, at(2026, 11, 10, 10, 0, 0)}, // 多个空格
	}
	for _, c := range cases {
		tr, err := NewTrigger(c.expr)
		if err != nil {
			t.Errorf("NewTrigger(%q) error: %v", c.expr, err)
			continue
		}
		got, ok := tr.Next(c.now)
		if !ok || !got.Equal(c.want) {
			t.Errorf("Next(%q, %s) = %s, %v; want %s",
				c.expr, c.now.Format(time.DateTime), got.Format(time.DateTime), ok, c.want.Format(time.DateTime))
		}
	}
}

func TestNextKeepsLocation(t *testing.T) {
	tr, err := NewTrigger("0 0 13 * * ?")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	got, ok := tr.Next(now)
	want := time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)
	if !ok || !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNextSequence(t *testing.T) {
	tr, err := NewTrigger("2/20 1,3 12-16 * * ?")
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Time{
		at(2026, 11, 9, 12, 1, 2), at(2026, 11, 9, 12, 1, 22), at(2026, 11, 9, 12, 1, 42),
		at(2026, 11, 9, 12, 3, 2), at(2026, 11, 9, 12, 3, 22), at(2026, 11, 9, 12, 3, 42),
		at(2026, 11, 9, 13, 1, 2),
	}
	now := base
	for i, w := range want {
		got, ok := tr.Next(now)
		if !ok || !got.Equal(w) {
			t.Fatalf("step %d: got %s, want %s", i, got.Format(time.DateTime), w.Format(time.DateTime))
		}
		now = got
	}
}

func TestTriggerConcurrentNext(t *testing.T) {
	tr, err := NewTrigger("0 0 12 ? * 6L")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(m int) {
			defer wg.Done()
			tr.Next(at(2026, m, 1, 0, 0, 0))
		}(i + 1)
	}
	wg.Wait()
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		"",
		"0 0 * * *",
		"0 0 0 * * ? *",
		"a-5 * * * * ?",
		"x,5 * * * * ?",
		"60 * * * * ?",
		"0 60 * * * ?",
		"0 0 24 * * ?",
		"0 0 30 * * ?",
		"0 0 0 0 * ?",
		"0 0 0 32 * ?",
		"0 0 0 * 0 ?",
		"0 0 0 * 13 ?",
		"0 0 0 * FOO ?",
		"0 0 0 ? * 7",
		"0 0 0 ? * FOO",
		"5-1 * * * * ?",
		"1-2-3 * * * * ?",
		"*/0 * * * * ?",
		"*/x * * * * ?",
		"1,,2 * * * * ?",
		"? * * * * ?",
		"0 0 0 * ? ?",
		"0 0 0 30 2 ?",
		"0 0 0 31 2,4,6,9,11 ?",
		"0 0 0 W * ?",
		"0 0 0 40W * ?",
		"0 0 0 xW * ?",
		"0 0 0 1 * MON",
		"0 0 0 L * MON",
		"0 0 0 ? * 1#0",
		"0 0 0 ? * 1#6",
		"0 0 0 ? * 9#1",
		"0 0 0 ? * 9L",
	}
	for _, expr := range bad {
		if _, err := NewTrigger(expr); err == nil {
			t.Errorf("NewTrigger(%q) expected error", expr)
		}
	}
}

func waitCtx(t *testing.T, s *Scheduler) {
	t.Helper()
	select {
	case <-s.Stop().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not finish in time")
	}
}

func TestSchedulerRuns(t *testing.T) {
	t.Parallel()
	s := New()
	var n int64
	if _, err := s.AddJob("* * * * * ?", func() { atomic.AddInt64(&n, 1) }); err != nil {
		t.Fatal(err)
	}
	s.Start()
	time.Sleep(2200 * time.Millisecond)
	waitCtx(t, s)
	if c := atomic.LoadInt64(&n); c < 2 {
		t.Fatalf("job ran %d times, want >= 2", c)
	}
}

func TestSchedulerRemove(t *testing.T) {
	t.Parallel()
	s := New()
	var n int64
	id, err := s.AddJob("* * * * * ?", func() { atomic.AddInt64(&n, 1) })
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	s.Remove(id)
	time.Sleep(1500 * time.Millisecond)
	waitCtx(t, s)
	if c := atomic.LoadInt64(&n); c != 0 {
		t.Fatalf("removed job ran %d times", c)
	}
}

func TestSchedulerRestart(t *testing.T) {
	t.Parallel()
	s := New()
	var n int64
	if _, err := s.AddJob("* * * * * ?", func() { atomic.AddInt64(&n, 1) }); err != nil {
		t.Fatal(err)
	}
	s.Start()
	waitCtx(t, s)
	atomic.StoreInt64(&n, 0)
	s.Start()
	time.Sleep(2200 * time.Millisecond)
	waitCtx(t, s)
	if c := atomic.LoadInt64(&n); c < 2 {
		t.Fatalf("job ran %d times after restart, want >= 2", c)
	}
}

func TestSchedulerStopWaitsRunningJobs(t *testing.T) {
	t.Parallel()
	s := New()
	started := make(chan struct{})
	var once sync.Once
	var finished int32
	if _, err := s.AddJob("* * * * * ?", func() {
		once.Do(func() { close(started) })
		time.Sleep(300 * time.Millisecond)
		atomic.StoreInt32(&finished, 1)
	}); err != nil {
		t.Fatal(err)
	}
	s.Start()
	<-started
	waitCtx(t, s)
	if atomic.LoadInt32(&finished) != 1 {
		t.Fatal("Stop context done before running job finished")
	}
}

func TestSchedulerPanicRecovered(t *testing.T) {
	t.Parallel()
	s := New()
	var n int64
	if _, err := s.AddJob("* * * * * ?", func() { panic("boom") }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddJob("* * * * * ?", func() { atomic.AddInt64(&n, 1) }); err != nil {
		t.Fatal(err)
	}
	s.Start()
	time.Sleep(2200 * time.Millisecond)
	waitCtx(t, s)
	if c := atomic.LoadInt64(&n); c < 2 {
		t.Fatalf("healthy job ran %d times, want >= 2", c)
	}
}

func TestSchedulerConcurrentAddRemove(t *testing.T) {
	t.Parallel()
	s := New()
	s.Start()
	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := map[uint]bool{}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := s.AddJob("* * * * * ?", func() {})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			if ids[id] {
				t.Errorf("duplicate id %d", id)
			}
			ids[id] = true
			mu.Unlock()
			if i%2 == 0 {
				s.Remove(id)
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(1200 * time.Millisecond)
	waitCtx(t, s)
	// Stop 之后再添加不应 panic
	if _, err := s.AddJob("* * * * * ?", func() {}); err != nil {
		t.Fatal(err)
	}
}

// maxConcurrency 每秒触发一个耗时 1.5s 的任务，运行 3.2s，返回同时执行的最大数量。
func maxConcurrency(t *testing.T, opts ...JobOption) (maxCur, runs int64) {
	s := New()
	var cur int64
	if _, err := s.AddJob("* * * * * ?", func() {
		atomic.AddInt64(&runs, 1)
		c := atomic.AddInt64(&cur, 1)
		for {
			m := atomic.LoadInt64(&maxCur)
			if c <= m || atomic.CompareAndSwapInt64(&maxCur, m, c) {
				break
			}
		}
		time.Sleep(1500 * time.Millisecond)
		atomic.AddInt64(&cur, -1)
	}, opts...); err != nil {
		t.Fatal(err)
	}
	s.Start()
	time.Sleep(3200 * time.Millisecond)
	waitCtx(t, s)
	return atomic.LoadInt64(&maxCur), atomic.LoadInt64(&runs)
}

func TestSkipIfStillRunning(t *testing.T) {
	t.Parallel()
	maxCur, runs := maxConcurrency(t, SkipIfStillRunning())
	if maxCur != 1 {
		t.Fatalf("max concurrency = %d, want 1", maxCur)
	}
	if runs < 1 || runs > 2 {
		t.Fatalf("runs = %d, want 1-2", runs)
	}
}

func TestOverlapByDefault(t *testing.T) {
	t.Parallel()
	if maxCur, _ := maxConcurrency(t); maxCur < 2 {
		t.Fatalf("max concurrency = %d, want >= 2 without SkipIfStillRunning", maxCur)
	}
}

func TestAddJobInvalid(t *testing.T) {
	s := New()
	if _, err := s.AddJob("bad", func() {}); err == nil {
		t.Fatal("expected error for invalid expression")
	}
	if _, err := s.AddJob("* * * * * ?", nil); err == nil {
		t.Fatal("expected error for nil func")
	}
}
