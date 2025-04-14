package cron

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestCron(t *testing.T) {
	s := New()
	s.Start()
	id, err := s.AddJob("2/1 * 8-16 * 7 ?", func() {
		log.Println("1执行了2/1 * 8-16 * 7 ?")
	})
	if err != nil {
		panic(err)
	}
	log.Println("id=", id)
	time.Sleep(time.Second * 2)
	id, err = s.AddJob("2/1 0,10,20,30,40,50 * ? 7 1-2", func() {
		log.Println("执行了2/1 0,10,20,30,40,50 * ? 7 1-2")
	})
	if err != nil {
		panic(err)
	}
	log.Println("id=", id)
	s.Remove(1)
	s.Remove(2)
	time.Sleep(time.Second * 2)
	id, err = s.AddJob("2/2 * 8-17 * * ?", func() {
		log.Println("2执行了2/2 * 8-16 * * ?")
	})
	if err != nil {
		panic(err)
	}
	log.Println("id=", id)
	var ch chan struct{}
	<-ch
}

func TestGetNext10Times(t *testing.T) {
	trigger, err := NewTrigger("2/20 1,3 12-16 * * ?")
	if err != nil {
		panic(err)
	}
	now := time.Now()
	for i := 0; i < 10; i++ {
		tempNextTime := trigger.Next(now)
		fmt.Println(tempNextTime.Format("2006-01-02 15:04:05"))
		now = *tempNextTime
	}
}