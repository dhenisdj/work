package helper

import (
	"bytes"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/gomodule/redigo/redis"
	"reflect"
	"strconv"
	"time"
)

type TstCtx struct {
	a int
	bytes.Buffer
}

func (c *TstCtx) Record(s string) {
	_, _ = c.WriteString(s)
}

var TstCtxType = reflect.TypeOf(TstCtx{})

type TestContext struct{}

// Test Helpers
func (t *TestContext) SleepyJob(job *task.Job) error {
	sleepTime := time.Duration(job.ArgInt64("sleep"))
	time.Sleep(sleepTime * time.Millisecond)
	return nil
}

func ReadHash(pool *redis.Pool, key string) map[string]string {
	m := make(map[string]string)

	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Strings(conn.Do("HGETALL", key))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}

	for i, l := 0, len(v); i < l; i += 2 {
		m[v[i]] = v[i+1]
	}

	return m
}

func CleanKeyspace(namespace string, pool *redis.Pool) {
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

func NewTestPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxActive:   10,
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		Wait: true,
	}
}

func ListSize(pool *redis.Pool, key string) int64 {
	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("LLEN", key))
	if err != nil {
		panic("could not get list length: " + err.Error())
	}
	return v
}

func ZsetSize(pool *redis.Pool, key string) int64 {
	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("ZCARD", key))
	if err != nil {
		panic("could not get ZSET size: " + err.Error())
	}
	return v
}

func GetInt64(pool *redis.Pool, key string) int64 {
	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("GET", key))
	if err != nil {
		panic("could not GET int64: " + err.Error())
	}
	return v
}

func HgetInt64(pool *redis.Pool, redisKey, hashKey string) int64 {
	conn := pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("HGET", redisKey, hashKey))
	if err != nil {
		panic("could not HGET int64: " + err.Error())
	}
	return v
}

func JobOnZset(pool *redis.Pool, key string) (int64, *task.Job) {
	conn := pool.Get()
	defer conn.Close()

	v, err := conn.Do("ZRANGE", key, 0, 0, "WITHSCORES")
	if err != nil {
		panic("ZRANGE error: " + err.Error())
	}

	vv := v.([]interface{})

	job, err := task.NewJob(vv[0].([]byte), nil, nil)
	if err != nil {
		panic("couldn't get task: " + err.Error())
	}

	score := vv[1].([]byte)
	scoreInt, err := strconv.ParseInt(string(score), 10, 64)
	if err != nil {
		panic("couldn't parse int: " + err.Error())
	}

	return scoreInt, job
}

func JobOnQueue(pool *redis.Pool, key string) *task.Job {
	conn := pool.Get()
	defer conn.Close()

	rawJSON, err := redis.Bytes(conn.Do("RPOP", key))
	if err != nil {
		panic("could RPOP from task queue: " + err.Error())
	}

	job, err := task.NewJob(rawJSON, nil, nil)
	if err != nil {
		panic("couldn't get task: " + err.Error())
	}

	return job
}

func KnownJobs(pool *redis.Pool, key string) []string {
	conn := pool.Get()
	defer conn.Close()

	jobNames, err := redis.Strings(conn.Do("SMEMBERS", key))
	if err != nil {
		panic(err)
	}
	return jobNames
}

func PauseJobs(namespace, jobName string, pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()

	if _, err := conn.Do("SET", models.RedisKey2JobPaused(namespace, jobName), "1"); err != nil {
		return err
	}
	return nil
}

func UnpauseJobs(namespace, jobName string, pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()

	if _, err := conn.Do("DEL", models.RedisKey2JobPaused(namespace, jobName)); err != nil {
		return err
	}
	return nil
}

func DeletePausedAndLockedKeys(namespace, jobName string, pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()

	if _, err := conn.Do("DEL", models.RedisKey2JobPaused(namespace, jobName)); err != nil {
		return err
	}
	if _, err := conn.Do("DEL", models.RedisKey2JobLock(namespace, jobName)); err != nil {
		return err
	}
	if _, err := conn.Do("DEL", models.RedisKey2JobLockInfo(namespace, jobName)); err != nil {
		return err
	}
	return nil
}

func DeleteQueue(pool *redis.Pool, namespace, jobName string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", models.RedisKey2Job(namespace, jobName), models.RedisKey2JobInProgress(namespace, "1", jobName))
	if err != nil {
		panic("could not delete queue: " + err.Error())
	}
}

func DeleteRetryAndDead(pool *redis.Pool, namespace string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", models.RedisKey2JobRetry(namespace), models.RedisKey2JobDead(namespace))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
}
