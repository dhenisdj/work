package config

import (
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/spf13/viper"
	"path"
	"strings"
	"time"
)

const (
	ConfigS3            = "s3"
	ConfigKafka         = "kafka"
	ConfigCrm           = "crm"
	ConfigAm            = "am"
	ConfigAirflow       = "airflow"
	ConfigSpark         = "spark"
	ConfigSparkConf     = "spark.sparkConf"
	ConfigHiveConf      = "spark.hiveConf"
	ConfigSparkExecutor = "spark.executor"

	URIInternalTaskPoll   = "/internal/task/poll"
	URIInternalTaskUpdate = "/internal/task"

	URIDependenciesPoll   = "/open/dep_table/list"
	URIDependenciesUpdate = "/open/dep_table/update_batch"

	SchedulerNamespace = "am"
	DefaultRegion      = "sg"
	PoolKindFetcher    = "fp"
	PoolKindWorker     = "wp"
	GroupKindFetcher   = "fg"
	GroupKindWorker    = "wg"

	DefaultFetcherConcurrency = 3

	NameSpacePools        = "ps"
	NameSpaceGroups       = "gs"
	NameSpaceFetcherPool  = "fp"
	NameSpaceFetcherGroup = "fg"
	NameSpaceFetcher      = "f"
	NameSpaceEnqueue      = "enq"
	NameSpaceWorkerPool   = "wp"
	NameSpaceWorkerGroup  = "wg"
	NameSpaceWorker       = "w"
	NameSpaceJobType      = "jts"
	NameSpaceJob          = "jbs"

	HeartbeatPeriod = 5 * time.Second

	RequeueKeysPerJob   = 4
	FetchKeysPerJobType = 6

	HeartbeatId             = "id"
	HeartbeatKind           = "kind"
	HeartbeatStart          = "started_at"
	HeartbeatBeat           = "heartbeat_at"
	HeartbeatJobConcurrency = "jobConcurrency"
	HeartbeatExecutorIds    = "executorIds"
	HeartbeatIp             = "ip"
	HeartbeatPid            = "pid"
	HeartbeatHost           = "host"
	HeartbeatJobNames       = "job_names"
)

var SleepBackoffsInMilliseconds = []int64{0, 10, 100, 1000, 5000}

var FinalArgs = []string{
	"file",
	"args",
	"name",
	"conf",
	"jars",
	"queue",
	"pyFiles",
	"archives",
	"numExecutors",
	"driverCores",
	"driverMemory",
	"executorCores",
	"executorMemory",
}

func InitConfig(env string) *entities.Configuration {
	if utils.Contain(env, "_") {
		panic("Configuration initialization key env must be {env}_{cid} format\n")
	}
	envs := []string{"test", "uat", "live"}
	envRegion := strings.Split(env, "_")
	envWithNoRegion := envRegion[0]
	region := DefaultRegion
	if len(envRegion) > 1 {
		region = envRegion[1]
	}
	flag := utils.Contain(envs, envWithNoRegion)
	if !flag {
		panic(fmt.Errorf("required env should be %v, but %s provided", envs, env))
	}

	configName := fmt.Sprintf("config_%v.yaml", envWithNoRegion)
	configPath := path.Join("./config", configName)
	config := viper.New()
	config.SetConfigFile(configPath)
	err := config.ReadInConfig()
	if err != nil { // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %w \n", err))
	}

	// Build S3 configuration
	s3 := &entities.S3Config{}
	if err := config.UnmarshalKey(ConfigS3, s3); err != nil {
		panic(fmt.Errorf("Fatal error decoding S3 config: %w \n", err))
	}

	// Build Kafka configuration
	kfk := &entities.KafkaConfig{}
	if err := config.UnmarshalKey(ConfigKafka, kfk); err != nil {
		panic(fmt.Errorf("Fatal error decoding Kafka config: %w \n", err))
	}

	// Build CRM configuration
	crm := &entities.CRMConfig{}
	if err := config.UnmarshalKey(ConfigCrm, crm); err != nil {
		panic(fmt.Errorf("Fatal error decoding CRM config: %w \n", err))
	}

	// Build AM configuration
	am := &entities.AMConfig{}
	if err := config.UnmarshalKey(ConfigAm, am); err != nil {
		panic(fmt.Errorf("Fatal error decoding AM config: %w \n", err))
	}

	// Build AM configuration
	air := &entities.AirflowConfig{}
	if err := config.UnmarshalKey(ConfigAirflow, air); err != nil {
		panic(fmt.Errorf("Fatal error decoding Airflow config: %w \n", err))
	}

	// Build AM configuration
	deps := &entities.SparkDependency{}
	if err := config.UnmarshalKey(ConfigSpark, deps); err != nil {
		panic(fmt.Errorf("Fatal error decoding Spark Dependencies config: %w \n", err))
	}
	file := deps.File
	if find := strings.Contains(file, "%s"); find {
		deps.File = fmt.Sprintf(file, region)
	}
	for i, pyFile := range deps.PyFiles {
		if find := strings.Contains(pyFile, "%s"); find {
			deps.PyFiles[i] = fmt.Sprintf(pyFile, region)
		}
	}

	sparkConf := &entities.SparkConf{}
	err = json.Unmarshal([]byte(config.GetString(ConfigSparkConf)), &sparkConf)
	if err != nil {
		panic(fmt.Errorf("Fatal error decoding spark.sparkConf config: %w \n", err))
	}

	executor := &entities.Executor{}
	if err := config.UnmarshalKey(ConfigSparkExecutor, executor); err != nil {
		panic(fmt.Errorf("Fatal error decoding Spark.executor config: %w \n", err))
	}

	formattedExecutor := make(entities.Executor)
	if len(*executor) > 0 {
		for _, v := range *executor {
			formattedExecutor[v.Name] = v
		}
	}

	spark := &entities.SparkConfig{}
	spark.SparkConf = sparkConf
	spark.SparkDependency = deps
	spark.Executor = &formattedExecutor

	configuration := &entities.Configuration{
		S3:      s3,
		Kafka:   kfk,
		CRM:     crm,
		AM:      am,
		Ariflow: air,
		Spark:   spark,
		Env:     envWithNoRegion,
		Region:  region,
	}

	return configuration

}
