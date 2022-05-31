package main

import (
	"flag"
	"github.com/dhenisdj/scheduler/component/actors/pool"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/config"
)

func main() {
	flag.Parse()

	env := "test_my"

	// Context
	cleanRedis := false
	ctx := context.New(env, cleanRedis)

	ctx.I("start AudienceManager Scheduler")

	// Build Fetcher and Fetch
	fp := pool.NewFetcherPool(ctx)
	wp := pool.NewWorkerPool(ctx)

	fp.Start(config.URIInternalTaskPoll)
	wp.Start()

	select {}
}
