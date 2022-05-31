package task

import (
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/execute/observe"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/robfig/cron/v3"
	"math"
	"math/rand"
	"reflect"
	"time"
)

// Job represents a task.
type Job struct {
	// Inputs when making a new task
	Name       string                 `json:"name,omitempty"`
	Account    entities.Account       `json:"account"`
	ID         string                 `json:"id"`
	SessionID  int                    `json:"sessionID"`
	EnqueuedAt int64                  `json:"t"`
	Args       map[string]interface{} `json:"args"`
	Unique     bool                   `json:"unique,omitempty"`
	UniqueKey  string                 `json:"unique_key,omitempty"`

	// Inputs when retrying
	Fails    int64  `json:"fails,omitempty"` // number of times this task has failed
	LastErr  string `json:"err,omitempty"`
	FailedAt int64  `json:"failed_at,omitempty"`

	RawJSON      []byte
	DequeuedFrom []byte
	InProgQueue  []byte
	argError     error
	Observer     *observe.Observer
}

type PeriodicJob struct {
	JobName  string
	Spec     string
	Schedule cron.Schedule
}

type ScheduledPeriodicJob struct {
	scheduledAt      time.Time
	scheduledAtEpoch int64
	*PeriodicJob
}

// Q is a shortcut to easily specify arguments for jobs when enqueueing them.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com", "track": true})
type Q map[string]interface{}

func NewJob(rawJSON, dequeuedFrom, inProgQueue []byte) (*Job, error) {
	var job Job
	err := json.Unmarshal(rawJSON, &job)
	if err != nil {
		return nil, err
	}
	job.RawJSON = rawJSON
	job.DequeuedFrom = dequeuedFrom
	job.InProgQueue = inProgQueue
	return &job, nil
}

func (j *Job) Serialize() ([]byte, error) {
	return json.Marshal(j)
}

// SetArg sets a single named argument on the task.
func (j *Job) SetArg(key string, val interface{}) {
	if j.Args == nil {
		j.Args = make(map[string]interface{})
	}
	j.Args[key] = val
}

func (j *Job) Failed(err error) {
	j.Fails++
	j.LastErr = err.Error()
	j.FailedAt = utils.NowEpochSeconds()
}

// Checkin will update the status of the executing task to the specified messages. This message is visible within the web UI. This is useful for indicating some sort of progress on very long running jobs. For instance, on a task that has to process a million records over the course of an hour, the task could call Checkin with the current task number every 10k jobs.
func (j *Job) Checkin(msg string) {
	if j.Observer != nil {
		j.Observer.ObserveCheckin(j.Name, j.ID, msg)
	}
}

// ArgString returns j.Args[key] typed to a string. If the key is missing or of the wrong type, it sets an argument error
// on the task. This function is meant to be used in the body of a task handling function while extracting arguments,
// followed by a single call to j.ArgError().
func (j *Job) ArgString(key string) string {
	v, ok := j.Args[key]
	if ok {
		typedV, ok := v.(string)
		if ok {
			return typedV
		}
		j.argError = typecastError("string", key, v)
	} else {
		j.argError = missingKeyError("string", key)
	}
	return ""
}

// ArgInt64 returns j.Args[key] typed to an int64. If the key is missing or of the wrong type, it sets an argument error
// on the task. This function is meant to be used in the body of a task handling function while extracting arguments,
// followed by a single call to j.ArgError().
func (j *Job) ArgInt64(key string) int64 {
	v, ok := j.Args[key]
	if ok {
		rVal := reflect.ValueOf(v)
		if isIntKind(rVal) {
			return rVal.Int()
		} else if isUintKind(rVal) {
			vUint := rVal.Uint()
			if vUint <= math.MaxInt64 {
				return int64(vUint)
			}
		} else if isFloatKind(rVal) {
			vFloat64 := rVal.Float()
			vInt64 := int64(vFloat64)
			if vFloat64 == math.Trunc(vFloat64) && vInt64 <= 9007199254740892 && vInt64 >= -9007199254740892 {
				return vInt64
			}
		}
		j.argError = typecastError("int64", key, v)
	} else {
		j.argError = missingKeyError("int64", key)
	}
	return 0
}

// ArgFloat64 returns j.Args[key] typed to a float64. If the key is missing or of the wrong type, it sets an argument error
// on the task. This function is meant to be used in the body of a task handling function while extracting arguments,
// followed by a single call to j.ArgError().
func (j *Job) ArgFloat64(key string) float64 {
	v, ok := j.Args[key]
	if ok {
		rVal := reflect.ValueOf(v)
		if isIntKind(rVal) {
			return float64(rVal.Int())
		} else if isUintKind(rVal) {
			return float64(rVal.Uint())
		} else if isFloatKind(rVal) {
			return rVal.Float()
		}
		j.argError = typecastError("float64", key, v)
	} else {
		j.argError = missingKeyError("float64", key)
	}
	return 0.0
}

// ArgBool returns j.Args[key] typed to a bool. If the key is missing or of the wrong type, it sets an argument error
// on the task. This function is meant to be used in the body of a task handling function while extracting arguments,
// followed by a single call to j.ArgError().
func (j *Job) ArgBool(key string) bool {
	v, ok := j.Args[key]
	if ok {
		typedV, ok := v.(bool)
		if ok {
			return typedV
		}
		j.argError = typecastError("bool", key, v)
	} else {
		j.argError = missingKeyError("bool", key)
	}
	return false
}

// ArgError returns the last error generated when extracting typed params. Returns nil if extracting the args went fine.
func (j *Job) ArgError() error {
	return j.argError
}

func isIntKind(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Int || k == reflect.Int8 || k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64
}

func isUintKind(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Uint || k == reflect.Uint8 || k == reflect.Uint16 || k == reflect.Uint32 || k == reflect.Uint64
}

func isFloatKind(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Float32 || k == reflect.Float64
}

func missingKeyError(jsonType, key string) error {
	return fmt.Errorf("looking for a %s in task.Arg[%s] but key wasn't found", jsonType, key)
}

func typecastError(jsonType, key string, v interface{}) error {
	actualType := reflect.TypeOf(v)
	return fmt.Errorf("looking for a %s in task.Arg[%s] but value wasn't right type: %v(%v)", jsonType, key, actualType, v)
}

// Default algorithm returns an fastly increasing backoff counter which grows in an unbounded fashion
func DefaultBackoffCalculator(job *Job) int64 {
	fails := job.Fails
	return (fails * fails * fails * fails) + 15 + (rand.Int63n(30) * (fails + 1))
}
