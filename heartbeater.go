package work

import (
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

type poolHeartbeater struct {
	poolID         string        // Pool identifier
	namespace      string        // Namespace for redis for now eg, "am"
	pool           *redis.Pool   // Redis pool
	beatPeriod     time.Duration // Heartbeat duration
	totalCurrency  int           // Total concurrency for the whole pool
	concurrencyMap []byte        // Json format {"name":concurrency}
	jobNames       string        // String format for all job names separate by ,
	startedAt      int64         // Timestamp for this heartbeat
	pid            int           // Process id
	hostname       string        // Host name
	executorIDs    string        // All ids for the executor eg, fetcherIDs workerIDs

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newPoolHeartbeater(namespace, poolID string, totalCurrency int, pool *redis.Pool, jobTypes map[string]*jobType, concurrencyMap []byte, ids []string) *poolHeartbeater {
	h := &poolHeartbeater{
		poolID:           poolID,
		namespace:        namespace,
		totalCurrency:    totalCurrency,
		pool:             pool,
		beatPeriod:       HeartbeatPeriod,
		concurrencyMap:   concurrencyMap,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}

	jobNames := make([]string, 0, len(jobTypes))
	for k := range jobTypes {
		jobNames = append(jobNames, k)
	}
	sort.Strings(jobNames)
	h.jobNames = strings.Join(jobNames, ",")

	sort.Strings(ids)
	h.executorIDs = strings.Join(ids, ",")

	h.pid = os.Getpid()
	host, err := os.Hostname()
	if err != nil {
		logError("heartbeat.hostname", err)
		host = "hostname_errored"
	}
	h.hostname = host

	return h
}

func (h *poolHeartbeater) start(kind string) {
	go h.loop(kind)
}

func (h *poolHeartbeater) stop() {
	h.stopChan <- struct{}{}
	<-h.doneStoppingChan
}

func (h *poolHeartbeater) loop(kind string) {
	h.startedAt = nowEpochSeconds()
	h.heartbeat(kind) // do it right away
	ticker := time.Tick(h.beatPeriod)
	for {
		select {
		case <-h.stopChan:
			h.removeHeartbeat(kind)
			h.doneStoppingChan <- struct{}{}
			return
		case <-ticker:
			h.heartbeat(kind)
		}
	}
}

func (h *poolHeartbeater) heartbeat(kind string) {
	conn := h.pool.Get()
	defer conn.Close()

	poolsKey := redisKeyPools(h.namespace, kind)
	heartbeatKey := redisKeyHeartbeats(h.namespace, kind, h.poolID)

	conn.Send("SADD", poolsKey, h.poolID)
	conn.Send("HMSET", heartbeatKey,
		"heartbeat_at", nowEpochSeconds(),
		"started_at", h.startedAt,
		"job_names", h.jobNames,
		"concurrences", h.concurrencyMap,
		"ids", h.executorIDs,
		"host", h.hostname,
		"pid", h.pid,
	)

	if err := conn.Flush(); err != nil {
		logError("heartbeat", err)
	}
}

func (h *poolHeartbeater) removeHeartbeat(kind string) {
	conn := h.pool.Get()
	defer conn.Close()

	poolsKey := redisKeyPools(h.namespace, kind)
	heartbeatKey := redisKeyHeartbeats(h.namespace, kind, h.poolID)

	conn.Send("SREM", poolsKey, h.poolID)
	conn.Send("DEL", heartbeatKey)

	if err := conn.Flush(); err != nil {
		logError("remove_heartbeat", err)
	}
}
