package work

import (
	"fmt"
	"strings"
)

type Parser struct {
	SparkConf *SparkConfig `json:"sparkConf"`
}

func newParser(sparkConf *SparkConfig) *Parser {
	return &Parser{
		sparkConf,
	}
}

func (p *Parser) transfer(task *BaseTask) {
	biz := task.BusinessGroup
	if strings.ToLower(biz) == "referral" {
		task.BusinessGroup = "ads_ops"
	}
}

func (p *Parser) validate(task *BaseTask) bool {
	executorFromAPI := task.Executor
	es := *p.SparkConf.Executor
	if executor, ok := es[executorFromAPI]; ok {
		if strings.Compare(executor.Name, executorFromAPI) != 0 {
			return false
		}
		p.transfer(task)
		bizFromAPI := task.BusinessGroup
		if !executor.Business.validateBusiness(bizFromAPI) {
			return false
		}
		return true
	}
	return false
}

func (p *Parser) parse(task *BaseTask) *Job {
	spark := &Task{}

	if !p.validate(task) {
		return nil
	}

	executorName := task.Executor
	bizName := task.BusinessGroup
	region := task.Region

	spark.BusinessGroup = bizName
	spark.Region = region
	spark.Executor = executorName
	spark.Context = task.Context

	es := *p.SparkConf.Executor
	businessResource := es[executorName]
	business := businessResource.Business
	queue := business.Queue[bizName]
	account := business.Account[bizName]

	resource := businessResource.Resource
	spark.SparkResource = resource

	spark.Name = fmt.Sprintf("%s.%s.%s.%s.%d.%s", task.Env, executorName, bizName, region, task.Id, makeSuffix())
	spark.Business = bizName
	spark.Account = account
	spark.Queue = queue
	spark.SparkDependency = p.SparkConf.SparkDependency
	spark.SparkConf = p.SparkConf.SparkConf

	return spark.convert()
}
