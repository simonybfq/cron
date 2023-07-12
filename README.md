## Feature
```
字段	允许值	          允许的特殊字符
秒	0-59	          – * / ,
分	0-59	          – * / ,
小时	0-23	          – * / ,
日期	1-31              – * ? / , L W
月份	1-12 或者 JAN-DEC  – * / ,
星期	0-6 或者 SUN-SAT	  – * ? / , L #
```
## Install
```
go get github.com/simonybfq/cron
```
## Usage
```go
	s := New()
	id, err := s.AddJob("2/1 * 8-16 * * ?", func() {
		log.Println("1执行了2/1 * 8-16 * * ?")
	})
	if err != nil {
		panic(err)
	}
	log.Println("id=", id)
	go func() {
		time.Sleep(time.Second * 2)
		id, err = s.AddJob("2/1 0,10,20,30,40,50 * ? 7 1-2", func() {
			log.Println("执行了2/1 0,10,20,30,40,50 * ? 7 1-2")
		})
		if err != nil {
			panic(err)
		}
		log.Println("id=", id)
		s.Remove(1)
		time.Sleep(time.Second * 2)
		id, err = s.AddJob("* * 8-16 * * ?", func() {
			log.Println("2执行了* * 8-16 * * ?")
		})
		if err != nil {
			panic(err)
		}
		log.Println("id=", id)
	}()
	s.Start()
	var ch chan struct{}
	<-ch
```