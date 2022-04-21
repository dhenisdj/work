package work

import "encoding/json"

type Task struct {
	Executor      string `json:"executor"`
	Context       string `json:"context"`
	BusinessGroup string `json:"businessGroup"`
	Region        string `json:"region"`

	Business string  `json:"business"`
	Account  Account `json:"account"`
	Queue    string  `json:"queue"`
	Name     string  `json:"name"`
	*SparkResource
	*SparkDependency
	*SparkConf
}

func (t *Task) convert() *Job {
	jobName := t.Executor

	b, _ := json.Marshal(t)

	var m map[string]interface{}

	_ = json.Unmarshal(b, &m)

	job := &Job{
		Name:       jobName,
		ID:         t.Name,
		EnqueuedAt: nowEpochSeconds(),
		Args:       m,
	}
	return job
}
