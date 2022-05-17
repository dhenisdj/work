package main

import (
	"encoding/json"
	"flag"
	"github.com/dhenisdj/scheduler/component/actors/pool"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/helper"
	"github.com/dhenisdj/scheduler/config"
	"time"

	"github.com/gomodule/redigo/redis"
)

var redisHostPort = flag.String("redi", ":6379", "redi hostport")
var redisNamespace = flag.String("ns", "work", "redi namespace")

func main() {
	flag.Parse()
	// Context
	ctx := context.New()

	redis := newPool(*redisHostPort)
	helper.CleanKeyspace(config.SchedulerNamespace, redis)

	env := "test_sg"
	//execute := "FilterAudienceUsers"
	//businessGroup := "crm"
	ctx.If("%s Starting AudienceManager pool and work pool", env)
	// Initialization for configuration
	configuration := config.InitConfig(env)
	c, _ := json.Marshal(configuration)
	ctx.If("%s Configuration %s", env, c)
	// Build Fetcher and Fetch
	fp := pool.NewFetcherPool(ctx, configuration, redis)
	fp.Start(config.URIInternalTaskPoll)
	ctx.If("%s Started FetcherPool with URI %s", env, config.URIInternalTaskPoll)

	//wp := pool.NewWorkerPool(ctx, configuration, redis)
	//wp.Start()

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
