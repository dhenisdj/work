package parse

import (
	"encoding/base64"
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/task"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"strings"
)

type Parser struct {
	SparkConf *entities.SparkConfig `json:"conf"`
	ctx       *context.Context
}

func NewParser(ctx *context.Context, sparkConf *entities.SparkConfig) *Parser {
	return &Parser{
		SparkConf: sparkConf,
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
	spark := task.New(p.ctx)

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
	account := business.Account[bizName]

	resource := *businessResource.Resource
	spark.SparkResource = resource

	spark.Name = fmt.Sprintf("%s.%s.%s.%s.%d", bTask.Env, region, executorName, bizName, bTask.Id)
	spark.Business = bizName
	spark.Account = account
	spark.Queue = queue
	spark.SparkDependency = *p.SparkConf.SparkDependency
	spark.SparkConf = *p.SparkConf.SparkConf

	spark.Args = append(spark.Args, "--context")
	spark.Args = append(spark.Args, base64.StdEncoding.EncodeToString([]byte(spark.Context)))

	if executorName != "SyncDistributionCommon" {
		spark.Args = append(spark.Args, "--biz")
		spark.Args = append(spark.Args, spark.BusinessGroup)
	} else {
		spark.Args = append(spark.Args, "--business_name")
		spark.Args = append(spark.Args, "REFERRAL")
	}
	return spark.Convert()
}
