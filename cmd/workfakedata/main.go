package main

import (
	"flag"
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	"github.com/dhenisdj/scheduler/component/actors/pool"
	"github.com/dhenisdj/scheduler/component/actors/task"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/config"
	"math/rand"
	"time"

	"github.com/gomodule/redigo/redis"
)

var redisHostPort = flag.String("redi", ":6379", "redi hostport")

func epsilonHandler(job *task.Job) error {
	fmt.Println("epsilon")
	time.Sleep(time.Second)

	if rand.Intn(2) == 0 {
		return fmt.Errorf("random error")
	}
	return nil
}

func main() {
	flag.Parse()
	fmt.Println("Installing some fake data")

	redis := newPool(*redisHostPort)
	cleanKeyspace(redis, config.SchedulerNamespace)

	// Enqueue some jobs:
	go func() {
		conn := redis.Get()
		defer conn.Close()
		conn.Do("SADD", config.SchedulerNamespace+":known_jobs", "foobar")
	}()

	go func() {
		for {
			en := enqueue.NewEnqueuer(context.New(), config.SchedulerNamespace, "", "", redis)
			for i := 0; i < 20; i++ {
				en.Enqueue("foobar", task.Q{"i": i})
			}

			time.Sleep(1 * time.Second)
		}
	}()

	configuration := config.InitConfig("test_sg")
	wp := pool.NewWorkerPool(context.New(), configuration, redis)
	wp.Job("foobar", epsilonHandler)
	wp.Start()

	select {}
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

func cleanKeyspace(pool *redis.Pool, namespace string) {
	conn := pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", namespace+"*"))
	if err != nil {
		panic("could not get keys: " + err.Error())
	}
	for _, k := range keys {
		if _, err := conn.Do("DEL", k); err != nil {
			panic("could not del: " + err.Error())
		}
	}
}
