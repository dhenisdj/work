package heartbeat

import (
	"github.com/dhenisdj/scheduler/component/actors/task"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

type PoolHeartbeater struct {
	poolID         string        // Pool identifier
	namespace      string        // Namespace for redis for now eg, "am"
	kind           string        // Kind for pool eg, ""
	pool           *redis.Pool   // Redis pool
	beatPeriod     time.Duration // Heartbeat duration
	concurrencyMap []byte        // Json format {"name":concurrency}
	jobNames       string        // String format for all task names separate by ,
	startedAt      int64         // Timestamp for this heartbeat
	pid            int           // Process id
	hostname       string        // Host name
	ip             string        // Host ip
	executorIDs    string        // All ids for the execute eg, fetcherIDs workerIDs
	ctx            *context.Context

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func NewPoolHeartbeater(ctx *context.Context, namespace, kind, poolID string, pool *redis.Pool, jobTypes map[string]*task.JobType, concurrencyMap []byte, ids []string) *PoolHeartbeater {
	h := &PoolHeartbeater{
		poolID:           poolID,
		namespace:        namespace,
		kind:             kind,
		pool:             pool,
		beatPeriod:       config.HeartbeatPeriod,
		concurrencyMap:   concurrencyMap,
		ctx:              ctx,
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
	hostName, err := os.Hostname()
	if err != nil {
		ctx.LE("heartbeat.hostname", err)
		hostName = "localhost"
	}
	h.hostname = hostName

	ip, err := utils.GetIP()
	if err != nil {
		ctx.LE("heartbeat.hostname", err)
		ip = "127.0.0.1"
	}
	h.ip = ip

	return h
}

func (h *PoolHeartbeater) Start() {
	go h.loop()
}

func (h *PoolHeartbeater) Stop() {
	h.stopChan <- struct{}{}
	<-h.doneStoppingChan
}

func (h *PoolHeartbeater) loop() {
	h.startedAt = utils.NowEpochSeconds()
	h.heartbeat() // do it right away
	ticker := time.Tick(h.beatPeriod)
	for {
		select {
		case <-h.stopChan:
			h.removeHeartbeat()
			h.doneStoppingChan <- struct{}{}
			return
		case <-ticker:
			h.heartbeat()
		}
	}
}

func (h *PoolHeartbeater) heartbeat() {
	conn := h.pool.Get()
	defer conn.Close()

	poolsKey := models.RedisKey2Pools(h.namespace)
	heartbeatKey := models.RedisKey2Heartbeat(h.namespace, h.poolID)

	conn.Send("SADD", poolsKey, h.poolID)
	conn.Send("HMSET", heartbeatKey,
		config.HeartbeatId, h.poolID,
		config.HeartbeatKind, h.kind,
		config.HeartbeatStart, h.startedAt,
		config.HeartbeatBeat, utils.NowEpochSeconds(),
		config.HeartbeatJobConcurrency, h.concurrencyMap,
		config.HeartbeatExecutorIds, h.executorIDs,
		config.HeartbeatIp, h.ip,
		config.HeartbeatPid, h.pid,
		config.HeartbeatHost, h.hostname,
		config.HeartbeatJobNames, h.jobNames,
	)

	if err := conn.Flush(); err != nil {
		h.ctx.LE("heartbeat", err)
	}
}

func (h *PoolHeartbeater) removeHeartbeat() {
	conn := h.pool.Get()
	defer conn.Close()

	poolsKey := models.RedisKey2Pools(h.namespace)
	heartbeatKey := models.RedisKey2Heartbeat(h.namespace, h.poolID)

	conn.Send("SREM", poolsKey, h.poolID)
	conn.Send("DEL", heartbeatKey)

	if err := conn.Flush(); err != nil {
		h.ctx.LE("remove_heartbeat", err)
	}
}
