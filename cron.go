package cron

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//表达式例子：
//
//0 * * * * ? 每1分钟触发一次
//0 0 * * * ? 每天每1小时触发一次
//0 0 10 * * ? 每天10点触发一次
//0 * 14 * * ? 在每天下午2点到下午2:59期间的每1分钟触发
//0 30 9 1 * ? 每月1号上午9点半
//0 15 10 15 * ? 每月15日上午10:15触发
//
//*/5 * * * * ? 每隔5秒执行一次
//0 */1 * * * ? 每隔1分钟执行一次
//0 0 5-15 * * ? 每天5-15点整点触发
//0 0/3 * * * ? 每三分钟触发一次
//0 0-5 14 * * ? 在每天下午2点到下午2:05期间的每1分钟触发
//0 0/5 14 * * ? 在每天下午2点到下午2:55期间的每5分钟触发
//0 0/5 14,18 * * ? 在每天下午2点到2:55期间和下午6点到6:55期间的每5分钟触发
//0 0/30 9-17 * * ? 朝九晚五工作时间内每半小时
//0 0 10,14,16 * * ? 每天上午10点，下午2点，4点
//
//0 0 12 ? * WED 表示每个星期三中午12点
//0 0 17 ? * TUES,THUR,SAT 每周二、四、六下午五点
//0 10,44 14 ? 3 WED 每年三月的星期三的下午2:10和2:44触发
//0 15 10 ? * MON-FRI 周一至周五的上午10:15触发
//
//0 0 23 L * ? 每月最后一天23点执行一次
//0 15 10 L * ? 每月最后一日的上午10:15触发
//0 15 10 ? * 6L 每月的最后一个星期六上午10:15触发
//
//0 15 10 ? * 6#3 每月的第三个星期五上午10:15触发

const maxDeep = 10

var (
	ranges = map[string][]uint{
		"sec":  {0, 59},
		"min":  {0, 59},
		"hour": {0, 23},
		"day":  {1, 31},
		"mon":  {1, 12},
		"week": {0, 6},
	}
	monsAlias = map[string]uint{
		"JAN": 1,
		"FEB": 2,
		"MAR": 3,
		"APR": 4,
		"MAY": 5,
		"JUN": 6,
		"JUL": 7,
		"AUG": 8,
		"SEP": 9,
		"OCT": 10,
		"NOV": 11,
		"DEC": 12,
	}
	weekAlias = map[string]uint{
		"SUN": 0,
		"MON": 1,
		"TUE": 2,
		"WED": 3,
		"THU": 4,
		"FRI": 5,
		"SAT": 6,
	}
)

/*
字段	允许值	允许的特殊字符
秒	0-59	– * / ,
分	0-59	– * / ,
小时	0-23	– * / ,
日期	1-31	– * ? / , L W
月份	1-12 或者 JAN-DEC	– * / ,
星期	0-6 或者 SUN-SAT	– * ? / , L #
*/
type trigger struct {
	cron string
	sec  *field
	min  *field
	hour *field
	day  *field
	mon  *field
	week *field
}
type field struct {
	isRange   bool
	start     uint
	end       uint
	increment uint
	values    []uint
	calculate func(year, month int)
}

func newTrigger(cronExpression string) (t *trigger, err error) {
	t = new(trigger)
	t.cron = cronExpression
	err = t.parse()
	return
}

// calculate next time to run. returns zero time(time.Time{}) if recursion call deep more than maxDeep
func (t *trigger) next(now time.Time, deeps ...uint8) *time.Time {
	var deep uint8
	if len(deeps) > 0 {
		deep = deeps[0]
		if deep > maxDeep {
			return &time.Time{}
		}
	}
	deep++
	year := uint(now.Year())
	month := uint(now.Month())
	weekDay := uint(now.Weekday())
	day := uint(now.Day())
	hour := uint(now.Hour())
	min := uint(now.Minute())
	sec := uint(now.Second())
	var (
		nextYear    = uint(now.Year())
		nextMonth   uint
		nextWeekDay uint
		nextDay     uint
		nextHour    uint
		nextMin     uint
		nextSec     uint
		isFind      bool
	)
	//月
	if t.mon.isRange {
		nextMonth, isFind = getRangeNextValue(t.mon.start, t.mon.end, month)
	} else {
		nextMonth, isFind = getIncreaseNextValue(t.mon.values, month)
	}
	if !isFind {
		nextYear++
	}
	//星期
	if t.week.calculate != nil {
		//计算星期和日
		t.week.calculate(int(nextYear), int(nextMonth))
		nextDay = t.day.start
		//如果算出来的要小于当前日期
		tempDate := time.Date(int(nextYear), time.Month(nextMonth), int(nextDay), int(hour), int(min), int(sec), now.Nanosecond(), time.Local)
		if tempDate.Before(now) {
			return t.next(time.Date(int(nextYear), time.Month(nextMonth), int(day), 0, 0, 0, 0, time.Local).AddDate(0, 1, 0), deep)
		}
	} else {
		//如果不是当月，则星期和日从起始值开始计算
		startWeekDay := weekDay
		startDay := day
		if !(nextYear == year && nextMonth == month) {
			startWeekDay = 0
			startDay = 1
		}
		if t.week.isRange {
			nextWeekDay, isFind = getRangeNextValue(t.week.start, t.week.end, startWeekDay)
		} else {
			nextWeekDay, isFind = getIncreaseNextValue(t.week.values, startWeekDay)
		}
		if !isFind {
			//下一个nextWeekDay
			tempDate := getMonthAfterLatestWeek(int(nextYear), int(nextMonth), int(startDay), int(nextWeekDay))
			return t.next(tempDate, deep)
		}
		//日,找出和星期对应的日
		if t.day.isRange {
			nextDay = getRangeDayNextValue(t.day.start, t.day.end, nextYear, nextMonth, startDay, nextWeekDay)
		} else {
			nextDay = getIncreaseDayNextValue(t.day.values, int(nextYear), nextMonth, startDay, nextWeekDay)
		}
	}
	//时,如果不是当前日,时从起始值算起
	startHour := hour
	if !(nextYear == year && nextMonth == month && nextDay == day) {
		startHour = 0
	}
	if t.hour.isRange {
		nextHour, isFind = getRangeNextValue(t.hour.start, t.hour.end, startHour)
	} else {
		nextHour, isFind = getIncreaseNextValue(t.hour.values, startHour)
	}
	if !isFind {
		return t.next(time.Date(int(nextYear), time.Month(nextMonth), int(nextDay), int(nextHour), 0, 0, 0, time.Local).Add(24*time.Hour), deep)
	}
	//分,如果不是当前小时,分从起始值算起
	startMin := min
	if !(nextYear == year && nextMonth == month && nextDay == day && nextHour == hour) {
		startMin = 0
	}
	if t.min.isRange {
		nextMin, isFind = getRangeNextValue(t.min.start, t.min.end, startMin)
	} else {
		nextMin, isFind = getIncreaseNextValue(t.min.values, startMin)
	}
	if !isFind {
		return t.next(time.Date(int(nextYear), time.Month(nextMonth), int(nextDay), int(nextHour), int(nextMin), 0, 0, time.Local).Add(time.Hour), deep)
	}
	//秒,如果不是当前分钟,秒从起始值算起
	startSec := sec
	if !(nextYear == year && nextMonth == month && nextDay == day && nextHour == hour && nextMin == min) {
		startSec = 0
	}
	if t.sec.isRange {
		nextSec, isFind = getRangeNextValue(t.sec.start, t.sec.end, startSec)
	} else {
		nextSec, isFind = getIncreaseNextValue(t.sec.values, startSec)
	}
	if !isFind {
		return t.next(time.Date(int(nextYear), time.Month(nextMonth), int(nextDay), int(nextHour), int(nextMin), int(nextSec), 0, time.Local).Add(time.Minute), deep)
	}
	nextTime := time.Date(int(nextYear), time.Month(nextMonth), int(nextDay), int(nextHour), int(nextMin), int(nextSec), 0, time.Local)
	return &nextTime
}

func (t *trigger) parserRangeField(unit string, arr []string, str string, f *field) error {
	tempRange := ranges[unit]
	min := tempRange[0]
	max := tempRange[1]
	start0, err := strconv.ParseUint(arr[0], 10, 8)
	if err != nil {
		return errors.New(fmt.Sprintf(unit+" %s start:%s is not a positive integer", str, arr[0]))
	}
	start := uint(start0)
	if start < min || start > max {
		return errors.New(fmt.Sprintf(unit+" range should be [%d,%d]", min, max))
	}
	end0, err := strconv.ParseUint(arr[1], 10, 8)
	if err != nil {
		return errors.New(fmt.Sprintf(unit+" %s end:%s is not a positive integer", str, arr[1]))
	}
	end := uint(end0)
	if end < min || end > max {
		return errors.New(fmt.Sprintf(unit+" range should be [%d,%d]", min, max))
	}
	if start > end {
		return errors.New(fmt.Sprintf(unit+" %s start:%d should not greater than end %d", str, start, end))
	}
	*f = field{isRange: true, start: start, end: end}
	return nil
}
func (t *trigger) parserIncreaseField(unit string, arr []string, str string, f *field) error {
	tempRange := ranges[unit]
	min := tempRange[0]
	max := tempRange[1]
	var (
		start     = min
		end       = max
		increment uint
	)
	if arr[0] != "*" {
		start0, err := strconv.ParseUint(arr[0], 10, 8)
		if err != nil {
			return errors.New(fmt.Sprintf(unit+" %s start:%s is not a positive integer", str, arr[0]))
		}
		start = uint(start0)
		if start < min || start > max {
			return errors.New(fmt.Sprintf(unit+" range should be [%d,%d]", min, max))
		}
	}
	increment0, err := strconv.ParseUint(arr[1], 10, 8)
	if err != nil {
		return errors.New(fmt.Sprintf(unit+" %s increment:%s is not a positive integer", str, arr[1]))
	}
	increment = uint(increment0)
	if increment == 0 {
		return errors.New(fmt.Sprintf(unit+" increment should > 0", min, max))
	}
	if increment < min || increment > max {
		return errors.New(fmt.Sprintf(unit+" range should be [%d,%d]", min, max))
	}
	*f = field{isRange: false, start: start, end: end, increment: increment}
	for i := start; i <= end; i += increment {
		f.values = append(f.values, i)
	}
	return nil
}
func (t *trigger) parserEnumField(unit string, arr []string, str string, f *field) error {
	tempRange := ranges[unit]
	min := tempRange[0]
	max := tempRange[1]
	var (
		tempNum uint
		values  []uint
	)
	for i := 0; i < len(arr); i++ {
		temp, err := strconv.ParseUint(arr[i], 10, 8)
		if err != nil {
			return errors.New(fmt.Sprintf(unit+" %s enum %s should be positive integer ", str, arr[i]))
		}
		tempNum = uint(temp)
		if tempNum < min || tempNum > max {
			return errors.New(fmt.Sprintf(unit+" range should be [%d,%d]", min, max))
		}
		values = append(values, tempNum)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	*f = field{isRange: false, values: values}
	return nil
}

// 日期	1-31	– * ? / , L W
func (t *trigger) parserDayField(s string) (err error) {
	t.day = new(field)
	if s == "*" || s == "?" {
		t.day = &field{isRange: true, start: 1, end: 31}
	} else if index := strings.IndexByte(s, '-'); index > 0 {
		tempUnitArr := strings.Split(s, "-")
		err = t.parserRangeField("day", tempUnitArr, s, t.day)
		if err != nil {
			return err
		}
	} else if index = strings.IndexByte(s, '/'); index > 0 {
		tempUnitArr := strings.Split(s, "/")
		err = t.parserIncreaseField("day", tempUnitArr, s, t.day)
		if err != nil {
			return err
		}
	} else if index = strings.IndexByte(s, ','); index > 0 {
		tempUnitArr := strings.Split(s, ",")
		err = t.parserEnumField("day", tempUnitArr, s, t.day)
		if err != nil {
			return err
		}
	} else if s == "L" {
		t.day.calculate = func(year, month int) {
			start := getYearMonthDays(year, month)
			t.day = &field{isRange: true, start: uint(start), end: uint(start)}
		}
	} else if s == "LW" {
		t.day.calculate = func(year, month int) {
			max := getYearMonthDays(year, month)
			start := getLatestWorkDay(year, month, max).Day()
			t.day = &field{isRange: true, start: uint(start), end: uint(start)}
		}
	} else if index = strings.IndexByte(s, 'W'); index > 0 {
		//1W
		day, _ := strconv.ParseUint(s[:index], 10, 8)
		t.day.calculate = func(year, month int) {
			start := getLatestWorkDay(year, month, int(day)).Day()
			t.day = &field{isRange: true, start: uint(start), end: uint(start)}
		}
	} else {
		day, _ := strconv.ParseUint(s, 10, 8)
		if day < 1 || day > 31 {
			return errors.New(fmt.Sprintf("day field %s should be in [1,31]", s))
		}
		t.day = &field{isRange: true, start: uint(day), end: uint(day)}
	}
	return nil
}

// 月份	1-12 或者 JAN-DEC	– * / ,
func (t *trigger) parserMonField(s string) (err error) {
	t.mon = new(field)
	if s == "*" {
		t.mon = &field{isRange: true, start: 1, end: 12}
	} else if index := strings.IndexByte(s, '-'); index > 0 {
		tempUnitArr := strings.Split(s, "-")
		err = t.parserRangeField("mon", tempUnitArr, s, t.mon)
		if err != nil {
			return err
		}
	} else if index = strings.IndexByte(s, '/'); index > 0 {
		tempUnitArr := strings.Split(s, "/")
		err = t.parserIncreaseField("mon", tempUnitArr, s, t.mon)
		if err != nil {
			return err
		}
	} else if index = strings.IndexByte(s, ','); index > 0 {
		tempUnitArr := strings.Split(s, ",")
		err = t.parserEnumField("mon", tempUnitArr, s, t.mon)
		if err != nil {
			return err
		}
	} else {
		var start uint
		mon, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			//JAN-DEC
			alias, ok := monsAlias[s]
			if !ok {
				return errors.New("month field should be [1,12] or JAN-DEC")
			}
			start = alias
		} else {
			start = uint(mon)
			if start < 1 || start > 12 {
				return errors.New(fmt.Sprintf("month field %s should be in [1,12]", s))
			}
		}
		t.mon = &field{isRange: true, start: start, end: start}
	}
	return
}

// 星期	0-6 或者 SUN-SAT	– * ? / , L #
func (t *trigger) parserWeekField(s string) (err error) {
	if s == "*" || s == "?" {
		t.week = &field{isRange: true, start: 0, end: 6}
	} else {
		t.week = new(field)
		//日必须为*或者?
		arr := strings.Split(t.cron, " ")
		if arr[3] != "*" && arr[3] != "?" {
			return errors.New("day field must be * or ? when the week is specific")
		}
		if index := strings.IndexByte(s, '-'); index > 0 {
			tempUnitArr := strings.Split(s, "-")
			err = t.parserRangeField("week", tempUnitArr, s, t.week)
			if err != nil {
				return err
			}
		} else if index = strings.IndexByte(s, '/'); index > 0 {
			tempUnitArr := strings.Split(s, "/")
			err = t.parserIncreaseField("week", tempUnitArr, s, t.week)
			if err != nil {
				return err
			}
		} else if index = strings.IndexByte(s, ','); index > 0 {
			tempUnitArr := strings.Split(s, ",")
			err = t.parserEnumField("week", tempUnitArr, s, t.week)
			if err != nil {
				return err
			}
		} else if index = strings.IndexByte(s, 'L'); index > 0 {
			var weekNum int
			//当月最后一个星期六
			if s == "L" {
				weekNum = 6
			} else {
				start, err := strconv.ParseUint(s[:index], 10, 8)
				if err != nil {
					return errors.New(fmt.Sprintf("week:%s L of left should be a positive integer", s))
				}
				if start < 0 || start > 6 {
					return errors.New(fmt.Sprintf("week:%s L of left should be in [0,6]", s))
				}
			}
			t.week.calculate = func(year, month int) {
				now := getMonthLatestWeek(year, month, weekNum)
				t.week = &field{isRange: true, start: uint(now.Weekday()), end: uint(now.Weekday())}
				t.day = &field{isRange: true, start: uint(now.Day()), end: uint(now.Day())}
			}
		} else if index = strings.IndexByte(s, '#'); index > 0 {
			// 0#2每月第2个星期0
			weekDay, err := strconv.ParseUint(s[:index], 10, 8)
			if err != nil {
				return errors.New(fmt.Sprintf("week:%s weekDay should be a positive integer", s))
			}
			if weekDay < 0 || weekDay > 6 {
				return errors.New(fmt.Sprintf("week:%s weekDay should in [0,6]", s))
			}
			weekNum, err := strconv.ParseUint(s[index+1:], 10, 8)
			if err != nil {
				return errors.New(fmt.Sprintf("week:%s weekNum should be a positive integer", s))
			}
			if weekDay < 1 || weekDay > 4 {
				return errors.New(fmt.Sprintf("week:%s weekNum should in [1,4]", s))
			}
			t.week.calculate = func(year, month int) {
				now := getMonthWeekByWeekNumDay(year, month, uint(weekNum), uint(weekDay))
				t.week = &field{isRange: true, start: uint(now.Weekday()), end: uint(now.Weekday())}
				t.day = &field{isRange: true, start: uint(now.Day()), end: uint(now.Day())}
			}
		} else {
			var start uint
			week, err := strconv.ParseUint(s, 10, 8)
			if err != nil {
				//SUN-SAT
				alias, ok := weekAlias[s]
				if !ok {
					return errors.New("month field should be [0,6] or SUN-SAT")
				}
				start = alias
			} else {
				start = uint(week)
				if start < 0 || start > 6 {
					return errors.New(fmt.Sprintf("week field %s should be in [0,6]", s))
				}
			}
			t.week = &field{isRange: true, start: start, end: start}
		}
	}
	return
}

func (t *trigger) parse() (err error) {
	arr := strings.Split(t.cron, " ")
	if len(arr) != 6 {
		return errors.New("cronExpression's fields count is not 6")
	}
	//解析秒，分，时
	units := []string{"sec", "min", "hour"}
	for i, unit := range units {
		str := arr[i]
		f := new(field)
		switch i {
		case 0:
			t.sec = f
		case 1:
			t.min = f
		case 2:
			t.hour = f
		}
		if str == "*" {
			*f = field{isRange: true, start: 0, end: 59}
		} else if index := strings.IndexByte(str, '-'); index > 0 {
			tempUnitArr := strings.Split(str, "-")
			err = t.parserRangeField(unit, tempUnitArr, str, f)
			if err != nil {
				return err
			}
		} else if index = strings.IndexByte(str, '/'); index > 0 {
			tempUnitArr := strings.Split(str, "/")
			err = t.parserIncreaseField(unit, tempUnitArr, str, f)
			if err != nil {
				return err
			}
		} else if index = strings.IndexByte(str, ','); index > 0 {
			tempUnitArr := strings.Split(str, ",")
			err = t.parserEnumField(unit, tempUnitArr, str, f)
			if err != nil {
				return err
			}
		} else {
			start, err := strconv.ParseUint(str, 10, 8)
			if err != nil {
				return errors.New(fmt.Sprintf(unit+" %s is not a positive integer", str))
			}
			if start < 0 || start > 59 {
				return errors.New(unit + " range should be [0,59]")
			}
			*f = field{isRange: true, start: uint(start), end: uint(start)}
		}
	}
	//解析日
	err = t.parserDayField(arr[3])
	if err != nil {
		return err
	}
	//解析月
	err = t.parserMonField(arr[4])
	if err != nil {
		return err
	}
	//解析周
	err = t.parserWeekField(arr[5])
	if err != nil {
		return err
	}
	return
}

func getRangeNextValue(start uint, end uint, nowValue uint) (nextValue uint, isFind bool) {
	for i := start; i <= end; i++ {
		if i >= nowValue {
			nextValue = i
			isFind = true
			return
		}
	}
	nextValue = start
	return
}
func getIncreaseNextValue(values []uint, nowValue uint) (nextValue uint, isFind bool) {
	for _, value := range values {
		if value >= nowValue {
			nextValue = value
			isFind = true
			return
		}
	}
	nextValue = values[0]
	return
}
func getRangeDayNextValue(start uint, end uint, year, nextMonth uint, day uint, nextWeekDay uint) uint {
	for i := start; i <= end; i++ {
		if i >= day {
			tempDate := time.Date(int(year), time.Month(nextMonth), int(i), 0, 0, 0, 0, time.Local)
			if uint(tempDate.Weekday()) == nextWeekDay {
				return i
			}
		}
	}
	return 0
}
func getIncreaseDayNextValue(values []uint, year int, nextMonth uint, day uint, nextWeekDay uint) uint {
	for _, i := range values {
		if i >= day {
			tempDate := time.Date(year, time.Month(nextMonth), int(i), 0, 0, 0, 0, time.Local)
			if uint(tempDate.Weekday()) == nextWeekDay {
				return i
			}
		}
	}
	return 0
}

func getYearMonthDays(year int, month int) int {
	day31 := map[int]struct{}{
		1:  {},
		3:  {},
		5:  {},
		7:  {},
		8:  {},
		10: {},
		12: {},
	}
	if _, ok := day31[month]; ok {
		return 31
	}
	day30 := map[int]struct{}{
		4:  {},
		6:  {},
		9:  {},
		11: {},
	}
	if _, ok := day30[month]; ok {
		return 30
	}
	if (year%4 == 0 && year%100 != 0) || year%400 == 0 {
		return 29
	}
	return 28
}
func getLatestWorkDay(year int, month int, day int) time.Time {
	t, _ := time.Parse("20060102", fmt.Sprintf("%d%d%d", year, month, day))
	for wd := t.Weekday(); wd == 0 || wd == 6; t = t.AddDate(0, 0, -1) {
		wd = t.Weekday()
	}
	return t
}
func getMonthLatestWeek(year, month, weekNum int) time.Time {
	max := getYearMonthDays(year, month)
	t := time.Date(year, time.Month(month), max, 0, 0, 0, 0, time.Local)
	for wd := t.Weekday(); int(wd) != weekNum; wd = t.Weekday() {
		t = t.AddDate(0, 0, -1)
	}
	return t
}
func getMonthAfterLatestWeek(year, month, day, weekNum int) time.Time {
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)
	for wd := t.Weekday(); int(wd) != weekNum; wd = t.Weekday() {
		t = t.AddDate(0, 0, 1)
	}
	return t
}
func getMonthWeekByWeekNumDay(year int, month int, weekNum uint, weekDay uint) time.Time {
	t := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.Local)
	var tempWeekNum uint = 0
	for tempWeekDay := t.Weekday(); int(t.Month()) == month; tempWeekDay = t.Weekday() {
		if uint(tempWeekDay) == weekDay {
			tempWeekNum++
			if tempWeekNum == weekNum {
				return t
			}
		}
		t = t.AddDate(0, 0, 1)
	}
	return t
}

type job struct {
	id       uint
	t        *trigger
	fun      func()
	nextTime *time.Time
	running  bool
}

func newJob(cronExpression string, f func()) (j *job, err error) {
	j = new(job)
	j.t, err = newTrigger(cronExpression)
	if err != nil {
		return nil, err
	}
	j.fun = f
	j.nextTime = j.t.next(time.Now())
	return
}
func (j *job) next(t time.Time) *time.Time {
	if j.nextTime != nil && j.nextTime.After(t) {
		return j.nextTime
	}
	j.nextTime = j.t.next(t)
	return j.nextTime
}

func (j *job) run() {
	j.running = true
	defer func() {
		j.running = false
		if err := recover(); err != nil {
			debug.PrintStack()
		}
	}()
	j.fun()
}

type Scheduler struct {
	timer       *time.Timer
	jobMap      map[uint]*job
	jobs        []*job
	lock        sync.Mutex
	stop        chan struct{}
	id          uint
	running     bool
	reStartChan chan struct{}
	wg          sync.WaitGroup
}

func New() (s *Scheduler) {
	s = new(Scheduler)
	s.jobMap = make(map[uint]*job, 0)
	return
}
func (c *Scheduler) AddJob(cronExpression string, f func()) (id uint, err error) {
	j, err := newJob(cronExpression, f)
	if err != nil {
		return 0, err
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.id++
	j.id = c.id
	c.jobs = append(c.jobs, j)
	c.jobMap[j.id] = j
	if c.running {
		c.reStartChan <- struct{}{}
	}
	return j.id, nil
}
func (c *Scheduler) Remove(id uint) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.jobMap, id)
	var i int
	for i = 0; i < len(c.jobs); i++ {
		if c.jobs[i].id == id {
			break
		}
	}
	c.jobs = append(c.jobs[:i], c.jobs[i+1:]...)
}
func (c *Scheduler) Start() {
	if c.running {
		return
	}
	c.stop = make(chan struct{}, 0)
	c.reStartChan = make(chan struct{}, 1)
	go c.reStart()
	c.running = true
	if len(c.jobs) == 0 {
		return
	}
	c.run()
}
func (c *Scheduler) Stop() (ctx context.Context) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.running {
		close(c.stop)
		close(c.reStartChan)
		c.running = false
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		c.wg.Wait()
		cancel()
	}()
	return
}

func (c *Scheduler) sortJob(now time.Time) {
	c.lock.Lock()
	defer c.lock.Unlock()
	sort.Slice(c.jobs, func(i, j int) bool {
		nextTime0 := c.jobs[i].next(now)
		nextTime1 := c.jobs[j].next(now)
		return nextTime0.Before(*nextTime1)
	})
}

func (c *Scheduler) run() {
	now := time.Now()
	nextTime := &now
	nextNextTime := *nextTime
	for {
		now = time.Now()
		c.sortJob(nextNextTime)
		nextTime = c.jobs[0].nextTime
		nextNextTime = nextTime.Add(time.Second)
		c.timer = time.NewTimer(nextTime.Sub(now))
		select {
		case _ = <-c.timer.C:
			for i, tempNextTime := 0, nextTime; tempNextTime.Equal(*nextTime) && i < len(c.jobs); {
				c.runJob(c.jobs[i])
				c.jobs[i].next(nextNextTime)
				i++
				if i < len(c.jobs) {
					tempNextTime = c.jobs[i].nextTime
				}
			}
			c.timer.Stop()
		case <-c.stop:
			c.timer.Stop()
			return
		}
	}
}
func (c *Scheduler) reStart() {
	for {
		select {
		case <-c.reStartChan:
			c.Stop()
			c.Start()
		case <-c.stop:
			return
		}
	}
}
func (c *Scheduler) runJob(j *job) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		j.run()
	}()
}
