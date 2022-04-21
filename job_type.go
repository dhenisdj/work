package work

import "reflect"

type jobType struct {
	Name string
	JobOptions

	IsGeneric      bool
	GenericHandler GenericHandler
	DynamicHandler reflect.Value
}

func (jt *jobType) calcBackoff(j *Job) int64 {
	if jt.Backoff == nil {
		return defaultBackoffCalculator(j)
	}
	return jt.Backoff(j)
}
