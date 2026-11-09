// Package cron 提供 6 字段（秒 分 时 日 月 周）的 cron 表达式解析与任务调度。
//
// 字段  允许值             允许的特殊字符
// 秒    0-59               , - * /
// 分    0-59               , - * /
// 小时  0-23               , - * /
// 日期  1-31               , - * ? / L W
// 月份  1-12 或 JAN-DEC    , - * /
// 星期  0-6 或 SUN-SAT     , - * ? / L #   （0 = 周日）
//
// 表达式例子：
//
//	0 * * * * ?            每1分钟触发一次
//	0 0 10 * * ?           每天10点触发一次
//	*/5 * * * * ?          每隔5秒执行一次
//	0 0/30 9-17 * * ?      朝九晚五工作时间内每半小时
//	0 15 10 ? * MON-FRI    周一至周五的上午10:15触发
//	0 15 10 ? * 6L         每月的最后一个星期六上午10:15触发
//	0 0 0 ? 5 0#2          5月的第二个星期天00:00:00触发
//	0 0 23 L * ?           每月最后一天23点执行一次
//	0 15 10 LW * ?         每月最后一个工作日的上午10:15触发
//	0 15 10 15W * ?        离每月15号最近的工作日（不跨月）上午10:15触发
//
// 日期和星期不能同时指定具体值，其中一个必须为 * 或 ?。
package cron

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/bits"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// maxSearchYears 是 Next 向后搜索的最大年数，超过则认为没有下一次触发时间。
const maxSearchYears = 50

// bitset 记录一个字段允许的取值，第 v 位为 1 表示允许 v。
type bitset uint64

func (b bitset) has(v int) bool {
	return v >= 0 && v < 64 && b&(1<<uint(v)) != 0
}

// nextFrom 返回 >= v 的最小允许值。
func (b bitset) nextFrom(v int) (int, bool) {
	if v < 0 {
		v = 0
	}
	if v >= 64 {
		return 0, false
	}
	m := uint64(b) >> uint(v) << uint(v)
	if m == 0 {
		return 0, false
	}
	return bits.TrailingZeros64(m), true
}

func fullBits(lo, hi int) bitset {
	var b bitset
	for v := lo; v <= hi; v++ {
		b |= 1 << uint(v)
	}
	return b
}

type fieldSpec struct {
	name   string
	lo, hi int
	alias  map[string]int
}

var (
	secSpec  = fieldSpec{name: "second", lo: 0, hi: 59}
	minSpec  = fieldSpec{name: "minute", lo: 0, hi: 59}
	hourSpec = fieldSpec{name: "hour", lo: 0, hi: 23}
	daySpec  = fieldSpec{name: "day", lo: 1, hi: 31}
	monSpec  = fieldSpec{name: "month", lo: 1, hi: 12, alias: map[string]int{
		"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
		"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
	}}
	weekSpec = fieldSpec{name: "week", lo: 0, hi: 6, alias: map[string]int{
		"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
	}}
)

func (f fieldSpec) errorf(field, format string, args ...interface{}) error {
	return fmt.Errorf("cron: %s field %q: %s", f.name, field, fmt.Sprintf(format, args...))
}

// value 解析单个取值（数字或别名）并校验范围。
func (f fieldSpec) value(field, s string) (int, error) {
	if n, err := strconv.Atoi(s); err == nil {
		if n < f.lo || n > f.hi {
			return 0, f.errorf(field, "value %d out of range [%d,%d]", n, f.lo, f.hi)
		}
		return n, nil
	}
	if v, ok := f.alias[strings.ToUpper(s)]; ok {
		return v, nil
	}
	return 0, f.errorf(field, "invalid value %q", s)
}

// parse 解析由逗号分隔的列表，每一项可以是 *、a、a-b、*/n、a/n、a-b/n。
func (f fieldSpec) parse(s string) (bitset, error) {
	var b bitset
	for _, part := range strings.Split(s, ",") {
		rng, step := part, ""
		if i := strings.IndexByte(part, '/'); i >= 0 {
			rng, step = part[:i], part[i+1:]
		}
		var start, end int
		switch {
		case rng == "*":
			start, end = f.lo, f.hi
		case strings.Contains(rng, "-"):
			ab := strings.Split(rng, "-")
			if len(ab) != 2 {
				return 0, f.errorf(s, "invalid range %q", rng)
			}
			var err error
			if start, err = f.value(s, ab[0]); err != nil {
				return 0, err
			}
			if end, err = f.value(s, ab[1]); err != nil {
				return 0, err
			}
			if start > end {
				return 0, f.errorf(s, "range start %d is greater than end %d", start, end)
			}
		default:
			v, err := f.value(s, rng)
			if err != nil {
				return 0, err
			}
			start, end = v, v
			if step != "" {
				end = f.hi // a/n 表示从 a 开始到最大值，每 n 一次
			}
		}
		inc := 1
		if step != "" {
			n, err := strconv.Atoi(step)
			if err != nil || n < 1 {
				return 0, f.errorf(s, "invalid step %q", step)
			}
			inc = n
		}
		for v := start; v <= end; v += inc {
			b |= 1 << uint(v)
		}
	}
	return b, nil
}

type dayKind uint8

const (
	dayBits           dayKind = iota // 普通取值集合
	dayLast                          // L：当月最后一天
	dayLastWorkday                   // LW：当月最后一个工作日
	dayNearestWorkday                // nW：离 n 号最近的工作日（不跨月）
)

type weekKind uint8

const (
	weekBits weekKind = iota // 普通取值集合
	weekLast                 // nL：当月最后一个星期 n
	weekNth                  // n#k：当月第 k 个星期 n
)

// Trigger 是解析后的 cron 表达式。解析完成后不可变，可以被多个 goroutine 并发使用。
type Trigger struct {
	expr string

	sec, min, hour, mon bitset

	day     bitset
	dayKind dayKind
	dayN    int

	week     bitset
	weekKind weekKind
	weekDay  int
	weekN    int
}

// NewTrigger 解析 6 字段 cron 表达式。字段之间可以用任意数量的空白分隔。
func NewTrigger(cronExpression string) (*Trigger, error) {
	fs := strings.Fields(cronExpression)
	if len(fs) != 6 {
		return nil, fmt.Errorf("cron: expression %q must have 6 fields, got %d", cronExpression, len(fs))
	}
	t := &Trigger{expr: cronExpression}
	var err error
	if t.sec, err = secSpec.parse(fs[0]); err != nil {
		return nil, err
	}
	if t.min, err = minSpec.parse(fs[1]); err != nil {
		return nil, err
	}
	if t.hour, err = hourSpec.parse(fs[2]); err != nil {
		return nil, err
	}
	if err = t.parseDay(fs[3]); err != nil {
		return nil, err
	}
	if t.mon, err = monSpec.parse(fs[4]); err != nil {
		return nil, err
	}
	if err = t.parseWeek(fs[5]); err != nil {
		return nil, err
	}
	if !isAny(fs[3]) && !isAny(fs[5]) {
		return nil, fmt.Errorf("cron: expression %q: day and week cannot both be specified, one of them must be * or ?", cronExpression)
	}
	if !t.possible() {
		return nil, fmt.Errorf("cron: expression %q: day and month combination never occurs", cronExpression)
	}
	return t, nil
}

// String 返回原始表达式。
func (t *Trigger) String() string { return t.expr }

func isAny(s string) bool { return s == "*" || s == "?" }

func (t *Trigger) parseDay(s string) error {
	switch {
	case s == "?":
		t.day = fullBits(1, 31)
	case s == "L":
		t.dayKind = dayLast
	case s == "LW":
		t.dayKind = dayLastWorkday
	case strings.HasSuffix(s, "W"):
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil || n < 1 || n > 31 {
			return daySpec.errorf(s, "W must follow a day in [1,31]")
		}
		t.dayKind = dayNearestWorkday
		t.dayN = n
	default:
		b, err := daySpec.parse(s)
		if err != nil {
			return err
		}
		t.day = b
	}
	return nil
}

func (t *Trigger) parseWeek(s string) error {
	switch {
	case s == "?":
		t.week = fullBits(0, 6)
	case s == "L":
		t.weekKind = weekLast
		t.weekDay = 6
	case len(s) > 1 && strings.HasSuffix(s, "L"):
		wd, err := weekSpec.value(s, s[:len(s)-1])
		if err != nil {
			return err
		}
		t.weekKind = weekLast
		t.weekDay = wd
	case strings.Contains(s, "#"):
		parts := strings.Split(s, "#")
		if len(parts) != 2 {
			return weekSpec.errorf(s, "invalid # expression")
		}
		wd, err := weekSpec.value(s, parts[0])
		if err != nil {
			return err
		}
		n, err := strconv.Atoi(parts[1])
		if err != nil || n < 1 || n > 5 {
			return weekSpec.errorf(s, "number after # must be in [1,5]")
		}
		t.weekKind = weekNth
		t.weekDay = wd
		t.weekN = n
	default:
		b, err := weekSpec.parse(s)
		if err != nil {
			return err
		}
		t.week = b
	}
	return nil
}

// possible 检查日期和月份的组合是否可能出现，例如 2月30日 永远不会出现。
func (t *Trigger) possible() bool {
	for m := 1; m <= 12; m++ {
		if !t.mon.has(m) {
			continue
		}
		dim := daysIn(2000, m) // 2000 年是闰年，2 月取 29 天
		switch t.dayKind {
		case dayBits:
			if t.day&fullBits(1, dim) != 0 {
				return true
			}
		case dayNearestWorkday:
			if t.dayN <= dim {
				return true
			}
		default:
			return true
		}
	}
	return false
}

// Next 返回严格晚于 now 的下一次触发时间（精确到秒），使用 now 所在的时区。
// 如果在 maxSearchYears 年内找不到，返回 false。
func (t *Trigger) Next(now time.Time) (time.Time, bool) {
	loc := now.Location()
	start := now.Add(time.Second - time.Duration(now.Nanosecond()))
	y, month, d := start.Date()
	h, mi, s := start.Clock()
	mo := int(month)
	limit := y + maxSearchYears

	for y <= limit {
		m, ok := t.mon.nextFrom(mo)
		if !ok {
			y, mo, d, h, mi, s = y+1, 1, 1, 0, 0, 0
			continue
		}
		if m != mo {
			mo, d, h, mi, s = m, 1, 0, 0, 0
		}
		dd, ok := t.nextDay(y, mo, d)
		if !ok {
			mo, d, h, mi, s = mo+1, 1, 0, 0, 0
			continue
		}
		if dd != d {
			d, h, mi, s = dd, 0, 0, 0
		}
		hh, ok := t.hour.nextFrom(h)
		if !ok {
			d, h, mi, s = d+1, 0, 0, 0
			continue
		}
		if hh != h {
			h, mi, s = hh, 0, 0
		}
		mm, ok := t.min.nextFrom(mi)
		if !ok {
			h, mi, s = h+1, 0, 0
			continue
		}
		if mm != mi {
			mi, s = mm, 0
		}
		ss, ok := t.sec.nextFrom(s)
		if !ok {
			mi, s = mi+1, 0
			continue
		}
		res := time.Date(y, time.Month(mo), d, h, mi, ss, 0, loc)
		// 夏令时切换时，本地时间可能不存在或被规整到其他时刻，跳过这种结果
		if res.Day() != d || res.Hour() != h || res.Minute() != mi || res.Second() != ss || !res.After(now) {
			s = ss + 1
			continue
		}
		return res, true
	}
	return time.Time{}, false
}

// nextDay 返回当月 >= d 的第一个满足日期和星期约束的日子。
func (t *Trigger) nextDay(y, m, d int) (int, bool) {
	dim := daysIn(y, m)
	for ; d <= dim; d++ {
		if t.matchDay(y, m, d, dim) && t.matchWeek(y, m, d, dim) {
			return d, true
		}
	}
	return 0, false
}

func (t *Trigger) matchDay(y, m, d, dim int) bool {
	switch t.dayKind {
	case dayLast:
		return d == dim
	case dayLastWorkday:
		return d == lastWorkday(y, m, dim)
	case dayNearestWorkday:
		return d == nearestWorkday(y, m, t.dayN, dim)
	default:
		return t.day.has(d)
	}
}

func (t *Trigger) matchWeek(y, m, d, dim int) bool {
	wd := weekday(y, m, d)
	switch t.weekKind {
	case weekLast:
		return wd == t.weekDay && d+7 > dim
	case weekNth:
		return wd == t.weekDay && (d-1)/7+1 == t.weekN
	default:
		return t.week.has(wd)
	}
}

func daysIn(y, m int) int {
	return time.Date(y, time.Month(m)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func weekday(y, m, d int) int {
	return int(time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Weekday())
}

func lastWorkday(y, m, dim int) int {
	switch weekday(y, m, dim) {
	case int(time.Saturday):
		return dim - 1
	case int(time.Sunday):
		return dim - 2
	}
	return dim
}

// nearestWorkday 返回离 n 号最近的工作日，不跨月；n 超出当月天数时返回 0。
func nearestWorkday(y, m, n, dim int) int {
	if n > dim {
		return 0
	}
	switch weekday(y, m, n) {
	case int(time.Saturday):
		if n == 1 {
			return 3 // 1 号是周六，取周一 3 号
		}
		return n - 1
	case int(time.Sunday):
		if n == dim {
			return n - 2 // 月末是周日，取周五
		}
		return n + 1
	}
	return n
}

type job struct {
	id   uint
	t    *Trigger
	fn   func()
	next time.Time
	ok   bool // 是否还有下一次触发时间

	skipIfRunning bool
	running       atomic.Bool
}

// JobOption 是 AddJob 的可选配置。
type JobOption func(*job)

// SkipIfStillRunning 表示到达触发时间时，如果该任务的上一次执行还没结束，就跳过本次触发。
func SkipIfStillRunning() JobOption {
	return func(j *job) { j.skipIfRunning = true }
}

// runState 对应一次 Start 到 Stop 之间的运行周期。
type runState struct {
	stop chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

// Scheduler 是任务调度器，所有方法都可以并发调用。
// 任务在各自的 goroutine 中执行，默认情况下同一个任务的上一次执行未结束时，
// 下一次仍会照常触发；需要跳过时使用 SkipIfStillRunning 选项。
type Scheduler struct {
	mu     sync.Mutex
	jobs   []*job
	nextID uint
	wake   chan struct{}
	state  *runState // nil 表示未运行
}

func New() *Scheduler {
	return &Scheduler{wake: make(chan struct{}, 1)}
}

// AddJob 添加任务并返回任务 ID，可以在调度器运行前或运行中调用。
func (c *Scheduler) AddJob(cronExpression string, f func(), opts ...JobOption) (uint, error) {
	if f == nil {
		return 0, errors.New("cron: job func is nil")
	}
	t, err := NewTrigger(cronExpression)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.nextID++
	j := &job{id: c.nextID, t: t, fn: f}
	for _, opt := range opts {
		opt(j)
	}
	j.next, j.ok = t.Next(time.Now())
	c.jobs = append(c.jobs, j)
	c.mu.Unlock()
	c.notify()
	return j.id, nil
}

// Remove 移除任务，已经在执行中的那一次不受影响。ID 不存在时什么也不做。
func (c *Scheduler) Remove(id uint) {
	c.mu.Lock()
	for i, j := range c.jobs {
		if j.id == id {
			copy(c.jobs[i:], c.jobs[i+1:])
			c.jobs[len(c.jobs)-1] = nil
			c.jobs = c.jobs[:len(c.jobs)-1]
			break
		}
	}
	c.mu.Unlock()
	c.notify()
}

// Start 启动调度器；已在运行时什么也不做。Stop 之后可以再次 Start。
func (c *Scheduler) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != nil {
		return
	}
	st := &runState{stop: make(chan struct{}), done: make(chan struct{})}
	c.state = st
	go c.run(st)
}

// Stop 停止调度器，不再触发新的任务。返回的 context 在调度循环退出、
// 且本次运行期间启动的任务全部执行完毕后被取消。
func (c *Scheduler) Stop() context.Context {
	c.mu.Lock()
	st := c.state
	c.state = nil
	if st != nil {
		close(st.stop)
	}
	c.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	if st == nil {
		cancel()
		return ctx
	}
	go func() {
		<-st.done
		st.wg.Wait()
		cancel()
	}()
	return ctx
}

func (c *Scheduler) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Scheduler) run(st *runState) {
	defer close(st.done)
	for {
		c.mu.Lock()
		select {
		case <-st.stop:
			c.mu.Unlock()
			return
		default:
		}
		now := time.Now()
		var earliest time.Time
		for _, j := range c.jobs {
			if !j.ok {
				continue
			}
			if !j.next.After(now) {
				c.startJob(st, j)
				// 从当前时间算下一次，错过的触发（例如系统休眠）不会补跑
				if j.next, j.ok = j.t.Next(now); !j.ok {
					continue
				}
			}
			if earliest.IsZero() || j.next.Before(earliest) {
				earliest = j.next
			}
		}
		c.mu.Unlock()

		var timer *time.Timer
		var timerC <-chan time.Time
		if !earliest.IsZero() {
			timer = time.NewTimer(time.Until(earliest))
			timerC = timer.C
		}
		select {
		case <-timerC:
		case <-c.wake:
		case <-st.stop:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// startJob 必须在持有 c.mu 时调用。
func (c *Scheduler) startJob(st *runState, j *job) {
	if j.skipIfRunning && !j.running.CompareAndSwap(false, true) {
		return // 上一次还没结束，跳过本次触发
	}
	st.wg.Add(1)
	go func() {
		defer st.wg.Done()
		defer func() {
			if j.skipIfRunning {
				j.running.Store(false)
			}
			if r := recover(); r != nil {
				log.Printf("cron: job %d panic: %v\n%s", j.id, r, debug.Stack())
			}
		}()
		j.fn()
	}()
}
