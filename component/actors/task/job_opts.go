package task

import (
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"reflect"
	"strings"
)

// BackoffCalculator The builtin backoff calculator provides an exponentially increasing wait function.
type BackoffCalculator func(job *Job) int64

// JobOptions can be passed to JobWithOptions.
type JobOptions struct {
	IsBatch        bool              // If true, the task is LIVY batch session task
	Priority       uint              // Priority from 1 to 10000
	MaxFails       uint              // 1: send straight to dead (unless SkipDead)
	SkipDead       bool              // If true, don't send failed jobs to the dead queue when retries are exhausted.
	MaxConcurrency uint              // Max number of jobs to keep in flight (default is 0, meaning no max)
	Backoff        BackoffCalculator // If not set, uses the default backoff algorithm
}

type Task struct {
	Executor      string `json:"executor"`
	Context       string `json:"context"`
	BusinessGroup string `json:"businessGroup"`
	Region        string `json:"region"`

	Business string           `json:"business"`
	Account  entities.Account `json:"account"`
	Queue    string           `json:"queue"`
	Name     string           `json:"name"`
	Args     []string         `json:"args"`
	entities.SparkResource
	entities.SparkDependency
	entities.SparkConf `json:"conf"`
}

func New() *Task {
	return &Task{}
}

func (t *Task) Convert() *Job {
	jobName := fmt.Sprintf("%s%s", t.Executor, strings.ToUpper(t.BusinessGroup))

	b, _ := json.Marshal(t)

	var m map[string]interface{}

	_ = json.Unmarshal(b, &m)

	finalArgs := make(map[string]interface{}, 13)

	for _, k := range config.FinalArgs {
		finalArgs[k] = m[k]
	}

	job := &Job{
		Name:       jobName,
		Account:    t.Account,
		ID:         t.Name,
		EnqueuedAt: utils.NowEpochSeconds(),
		Args:       finalArgs,
	}
	return job
}

// GenericHandler is a task handler without any custom context.
type GenericHandler func(*Job) error

type JobType struct {
	Name string
	JobOptions

	IsGeneric      bool
	GenericHandler GenericHandler
	DynamicHandler reflect.Value
}

func (jt *JobType) CalcBackoff(j *Job) int64 {
	if jt.Backoff == nil {
		return DefaultBackoffCalculator(j)
	}
	return jt.Backoff(j)
}
