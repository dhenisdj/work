package enqueue

import (
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/helper"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"testing"

	"github.com/stretchr/testify/assert"
)

var ctx = context.New()

func TestRequeue(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	tMock := utils.NowEpochSeconds() - 10
	utils.SetNowEpochSecondsMock(tMock)
	defer utils.ResetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ctx, ns, "", "", pool)
	_, err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("foo", 10, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("foo", 14, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("bar", 19, nil)
	assert.NoError(t, err)

	utils.ResetNowEpochSecondsMock()

	re := NewRequeuer(context.New(), ns, pool, models.RedisKey2JobScheduled(ns), []string{"wat", "foo", "bar"})
	re.Start()
	re.drain()
	re.Stop()

	assert.EqualValues(t, 2, helper.ListSize(pool, models.RedisKey2Job(ns, "wat")))
	assert.EqualValues(t, 1, helper.ListSize(pool, models.RedisKey2Job(ns, "foo")))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, "bar")))
	assert.EqualValues(t, 2, helper.ZsetSize(pool, models.RedisKey2JobScheduled(ns)))

	j := helper.JobOnQueue(pool, models.RedisKey2Job(ns, "foo"))
	assert.Equal(t, j.Name, "foo")

	// Because we mocked time to 10 seconds ago above, the task was put on the zset with t=10 secs ago
	// We want to ensure it's requeued with t=now.
	// On boundary conditions with the VM, nowEpochSeconds() might be 1 or 2 secs ahead of EnqueuedAt
	assert.True(t, (j.EnqueuedAt+2) >= utils.NowEpochSeconds())

}

func TestRequeueUnknown(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	tMock := utils.NowEpochSeconds() - 10
	utils.SetNowEpochSecondsMock(tMock)
	defer utils.ResetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ctx, ns, "", "", pool)
	_, err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)

	nowish := utils.NowEpochSeconds()
	utils.SetNowEpochSecondsMock(nowish)

	re := NewRequeuer(context.New(), ns, pool, models.RedisKey2JobScheduled(ns), []string{"bar"})
	re.Start()
	re.drain()
	re.Stop()

	assert.EqualValues(t, 0, helper.ZsetSize(pool, models.RedisKey2JobScheduled(ns)))
	assert.EqualValues(t, 1, helper.ZsetSize(pool, models.RedisKey2JobDead(ns)))

	rank, job := helper.JobOnZset(pool, models.RedisKey2JobDead(ns))

	assert.Equal(t, nowish, rank)
	assert.Equal(t, nowish, job.FailedAt)
	assert.Equal(t, "unknown task when requeueing", job.LastErr)
}
