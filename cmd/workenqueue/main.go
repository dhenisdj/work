package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/config"
	"os"
	"time"

	"github.com/gomodule/redigo/redis"
)

var redisHostPort = flag.String("redi", ":6379", "redi hostport")
var jobName = flag.String("task", "", "task name")
var jobArgs = flag.String("args", "{}", "task arguments")

func main() {
	flag.Parse()

	if *jobName == "" {
		fmt.Println("no task specified")
		os.Exit(1)
	}

	pool := newPool(*redisHostPort)

	var args map[string]interface{}
	err := json.Unmarshal([]byte(*jobArgs), &args)
	if err != nil {
		fmt.Println("invalid args:", err)
		os.Exit(1)
	}

	en := enqueue.NewEnqueuer(context.New(), config.SchedulerNamespace, "", "", pool)
	en.Enqueue(*jobName, args)
}

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxActive:   20,
		MaxIdle:     20,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			return c, nil
			//return redi.NewLoggingConn(c, log.New(os.Stdout, "", 0), "redi"), err
		},
		Wait: true,
		//TestOnBorrow: func(c redi.Conn, t time.Time) error {
		//	_, err := c.Do("PING")
		//	return err
		//},
	}
}
