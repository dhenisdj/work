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
	ConfigRedis         = "redis"
	ConfigSpark         = "spark"
	ConfigSparkConf     = "spark.sparkConf"
	ConfigExecutorApps  = "spark.apps"
	ConfigSparkExecutor = "spark.executor"
	ConfigSparkLivy     = "spark.livy"

	URIInternalTaskPoll   = "/internal/task/poll"
	URIInternalTaskUpdate = "/internal/task"

	URIDependenciesPoll   = "/open/dep_table/list"
	URIDependenciesUpdate = "/open/dep_table/update_batch"

	SchedulerNamespace = "{am}"
	DefaultRegion      = "sg"
	PoolKindFetcher    = "fp"
	PoolKindWorker     = "wp"

	DefaultFetcherConcurrency = 1
	DefaultAPIRetryCount      = 3

	NameSpacePools = "ps"
	NameSpaceJob   = "jbs"

	HeartbeatPeriod = 5 * time.Second

	RequeueKeysPerJob        = 4
	FetchKeysPerJobType      = 6
	CheckKeysIsJobReachLimit = 2

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
var SleepBackoffsInSeconds = []int64{0, 1, 3, 5, 7}

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
	envs := []string{"test", "uat", "live"}
	envRegion := strings.Split(env, "_")
	envWithNoRegion := strings.ToLower(envRegion[0])
	region := DefaultRegion
	if len(envRegion) > 1 {
		region = strings.ToLower(envRegion[1])
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

	// Build Redis configuration
	redis := &entities.RedisConfig{}
	if err := config.UnmarshalKey(ConfigRedis, redis); err != nil {
		panic(fmt.Errorf("Fatal error decoding Redis config: %w \n", err))
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

	appConf := &entities.ExecutorAppConf{}
	err = json.Unmarshal([]byte(config.GetString(ConfigExecutorApps)), &appConf)
	if err != nil {
		panic(fmt.Errorf("Fatal error decoding spark.apps config: %w \n", err))
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

	livy := &entities.Livy{}
	if err := config.UnmarshalKey(ConfigSparkLivy, livy); err != nil {
		panic(fmt.Errorf("Fatal error decoding Spark.livy config: %w \n", err))
	}

	spark := &entities.SparkConfig{}
	spark.SparkConf = sparkConf
	spark.ExecutorAppConf = appConf
	spark.SparkDependency = deps
	spark.Executor = &formattedExecutor
	spark.Livy = livy

	configuration := &entities.Configuration{
		NameSpace: fmt.Sprintf("{am_%s_%s}", envWithNoRegion, region),
		S3:        s3,
		Kafka:     kfk,
		CRM:       crm,
		AM:        am,
		Airflow:   air,
		Redis:     redis,
		Spark:     spark,
		Env:       envWithNoRegion,
		Region:    region,
	}

	return configuration

}
