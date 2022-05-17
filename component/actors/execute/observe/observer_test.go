package observe

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/helper"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"testing"

	"github.com/stretchr/testify/assert"
)

var ctx = context.New()

func TestObserverStarted(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace

	tMock := int64(1425263401)
	utils.SetNowEpochSecondsMock(tMock)
	defer utils.ResetNowEpochSecondsMock()

	observer := NewObserver(ctx, ns, pool, "abcd")
	observer.Start()
	observer.ObserveStarted("foo", "bar", task.Q{"a": 1, "b": "wat"})
	//observe.observeDone("foo", "bar", nil)
	observer.Drain()
	observer.Stop()

	h := helper.ReadHash(pool, models.RedisKey2Observation(ns, "abcd"))
	assert.Equal(t, "foo", h["job_name"])
	assert.Equal(t, "bar", h["job_id"])
	assert.Equal(t, fmt.Sprint(tMock), h["started_at"])
	assert.Equal(t, `{"a":1,"b":"wat"}`, h["args"])
}

func TestObserverStartedDone(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace

	tMock := int64(1425263401)
	utils.SetNowEpochSecondsMock(tMock)
	defer utils.ResetNowEpochSecondsMock()

	observer := NewObserver(ctx, ns, pool, "abcd")
	observer.Start()
	observer.ObserveStarted("foo", "bar", task.Q{"a": 1, "b": "wat"})
	observer.ObserveDone("foo", "bar", nil)
	observer.Drain()
	observer.Stop()

	h := helper.ReadHash(pool, models.RedisKey2Observation(ns, "abcd"))
	assert.Equal(t, 0, len(h))
}

func TestObserverCheckin(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace

	observer := NewObserver(ctx, ns, pool, "abcd")
	observer.Start()

	tMock := int64(1425263401)
	utils.SetNowEpochSecondsMock(tMock)
	defer utils.ResetNowEpochSecondsMock()
	observer.ObserveStarted("foo", "bar", task.Q{"a": 1, "b": "wat"})

	tMockCheckin := int64(1425263402)
	utils.SetNowEpochSecondsMock(tMockCheckin)
	observer.ObserveCheckin("foo", "bar", "doin it")
	observer.Drain()
	observer.Stop()

	h := helper.ReadHash(pool, models.RedisKey2Observation(ns, "abcd"))
	assert.Equal(t, "foo", h["job_name"])
	assert.Equal(t, "bar", h["job_id"])
	assert.Equal(t, fmt.Sprint(tMock), h["started_at"])
	assert.Equal(t, `{"a":1,"b":"wat"}`, h["args"])
	assert.Equal(t, "doin it", h["checkin"])
	assert.Equal(t, fmt.Sprint(tMockCheckin), h["checkin_at"])
}

func TestObserverCheckinFromJob(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace

	observer := NewObserver(ctx, ns, pool, "abcd")
	observer.Start()

	tMock := int64(1425263401)
	utils.SetNowEpochSecondsMock(tMock)
	defer utils.ResetNowEpochSecondsMock()
	observer.ObserveStarted("foo", "barbar", task.Q{"a": 1, "b": "wat"})

	tMockCheckin := int64(1425263402)
	utils.SetNowEpochSecondsMock(tMockCheckin)

	j := &task.Job{Name: "foo", ID: "barbar", Observer: observer}
	j.Checkin("sup")

	observer.Drain()
	observer.Stop()

	h := helper.ReadHash(pool, models.RedisKey2Observation(ns, "abcd"))
	assert.Equal(t, "foo", h["job_name"])
	assert.Equal(t, "barbar", h["job_id"])
	assert.Equal(t, fmt.Sprint(tMock), h["started_at"])
	assert.Equal(t, "sup", h["checkin"])
	assert.Equal(t, fmt.Sprint(tMockCheckin), h["checkin_at"])
}
