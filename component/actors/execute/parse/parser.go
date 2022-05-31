package parse

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/context"
	"strings"
)

type Parser struct {
	SparkConf *entities.SparkConfig `json:"conf"`
	ctx       context.Context
}

func NewParser(ctx context.Context) *Parser {
	return &Parser{
		SparkConf: ctx.CONF().Spark,
		ctx:       ctx,
	}
}

func (p *Parser) transfer(task *entities.BaseTask) {
	biz := task.BusinessGroup
	if strings.ToLower(biz) == "referral" {
		task.BusinessGroup = "ads_ops"
	}
}

func (p *Parser) validate(task *entities.BaseTask) bool {
	executorFromAPI := task.Executor
	es := *p.SparkConf.Executor
	if executor, ok := es[executorFromAPI]; ok {
		if strings.Compare(executor.Name, executorFromAPI) != 0 {
			return false
		}
		p.transfer(task)
		bizFromAPI := task.BusinessGroup
		if !executor.Business.ValidateBusiness(bizFromAPI) {
			return false
		}
		return true
	}
	return false
}

func (p *Parser) Parse(bTask *entities.BaseTask) *task.Job {
	spark := task.New()

	if !p.validate(bTask) {
		return nil
	}

	executorName := bTask.Executor
	bizName := bTask.BusinessGroup
	if executorName == "SyncDistributionCommon" && bizName == "ads_ops" {
		bizName = "REFERRAL"
	}
	region := bTask.Region

	spark.BusinessGroup = bizName
	spark.Region = region
	spark.Executor = executorName
	spark.Context = bTask.Context

	es := *p.SparkConf.Executor
	businessResource := es[executorName]
	business := businessResource.Business
	queue := business.Queue[bizName]
	account := *business.Account[bizName]

	resource := *businessResource.Resource
	spark.SparkResource = resource

	if bTask.Env == "live" || bTask.Env == "uat" {
		spark.Name = fmt.Sprintf("%s.%s.%d", p.ctx.CONF().Env, executorName+strings.ToUpper(bizName), bTask.Id)
	} else {
		spark.Name = fmt.Sprintf("%s.%s.%s.%d", p.ctx.CONF().Env, p.ctx.CONF().Region, executorName+strings.ToUpper(bizName), bTask.Id)
	}
	spark.Business = bizName
	spark.Account = account
	spark.Queue = queue
	spark.SparkDependency = *p.SparkConf.SparkDependency
	spark.SparkConf = *p.SparkConf.SparkConf

	var args []string
	args = append(args, "--env")
	if p.ctx.CONF().Env == "test" {
		args = append(args, fmt.Sprintf("test_%s", p.ctx.CONF().Region))
	} else {
		args = append(args, p.ctx.CONF().Env)
	}
	args = append(args, "--context")
	b, _ := json.Marshal(bTask)
	args = append(args, base64.StdEncoding.EncodeToString(b))
	args = append(args, "--app")
	args = append(args, (*p.ctx.CONF().Spark.ExecutorAppConf)[executorName])

	if executorName != "SyncDistributionCommon" {
		args = append(args, "--biz")
		args = append(args, spark.BusinessGroup)
	}

	spark.Args = args
	return spark.Convert()
}
