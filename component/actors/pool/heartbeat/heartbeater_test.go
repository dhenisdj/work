package heartbeat

import (
	"encoding/json"
	"github.com/dhenisdj/scheduler/component/actors/task"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/helper"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestHeartbeater(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace

	tMock := int64(1425263409)
	utils.SetNowEpochSecondsMock(tMock)
	defer utils.ResetNowEpochSecondsMock()

	jobTypes := map[string]*task.JobType{
		"foo": nil,
		"bar": nil,
	}
	concurrences, _ := json.Marshal(map[string]int{
		"foo": 10,
		"bar": 10,
	})

	heart := NewPoolHeartbeater(context.New(), ns, "", "abcd", pool, jobTypes, concurrences, []string{"ccc", "bbb"})
	heart.Start()

	time.Sleep(20 * time.Millisecond)

	assert.True(t, redisInSet(pool, models.RedisKey2Pools(ns), "abcd"))

	h := helper.ReadHash(pool, models.RedisKey2Heartbeat(ns, "abcd"))
	assert.Equal(t, "1425263409", h["heartbeat_at"])
	assert.Equal(t, "1425263409", h["started_at"])
	assert.Equal(t, "bar,foo", h["job_names"])
	assert.Equal(t, "bbb,ccc", h["worker_ids"])
	assert.Equal(t, "10", h["concurrency"])

	assert.True(t, h["pid"] != "")
	assert.True(t, h["host"] != "")

	heart.Stop()

	assert.False(t, redisInSet(pool, models.RedisKey2Pools(ns), "abcd"))
}

func redisInSet(pool *redis.Pool, key, member string) bool {
	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Bool(conn.Do("SISMEMBER", key, member))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
	return v
}
