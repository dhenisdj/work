package work

import (
	"encoding/json"
	"fmt"
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
	ConfigSparkConf     = "spark.conf"
	ConfigSparkExecutor = "spark.executor"

	FetcherPoolNamespace = "am"
	WorkerPoolNamespace  = FetcherPoolNamespace
	DefaultRegion        = "sg"
	PoolKindFetcher      = "fetcher"
	PoolKindWorker       = "worker"

	JobTypeSingle = "single"
	JobTypeBatch  = "batch"

	IdentifierTypeFetcherPool  = "fterp"
	IdentifierTypeFetcherGroup = "fterg"
	IdentifierTypeFetcher      = "fter"
	IdentifierTypeEnqueuer     = "eqer"
	IdentifierTypeJob          = "job"
	IdentifierTypeWorkerPool   = "wkerp"
	IdentifierTypeWorkerGroup  = "wkerg"
	IdentifierTypeWorker       = "wker"

	DefaultFetcherConcurrency = 3

	NameSpaceEnqueue      = IdentifierTypeEnqueuer
	NameSpaceFetcher      = IdentifierTypeFetcher
	NameSpaceFetcherGroup = IdentifierTypeFetcherGroup
	NameSpaceFetcherPool  = IdentifierTypeFetcherPool
	NameSpaceWorker       = IdentifierTypeWorker
	NameSpaceWorkerGroup  = IdentifierTypeWorkerGroup
	NameSpaceWorkerPool   = IdentifierTypeWorkerPool
	NameSpaceJob          = IdentifierTypeJob

	HeartbeatPeriod = 5 * time.Second
)

func InitConfig(env string) *Configuration {
	envs := []string{"test", "uat", "live"}
	envRegion := strings.Split(env, "_")
	envWithNoRegion := envRegion[0]
	region := DefaultRegion
	if len(envRegion) > 1 {
		region = envRegion[1]
	}
	flag, _ := Contain(envs, envWithNoRegion)
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
	s3 := &S3Config{}
	if err := config.UnmarshalKey(ConfigS3, s3); err != nil {
		panic(fmt.Errorf("Fatal error decoding S3 config: %w \n", err))
	}

	// Build Kafka configuration
	kfk := &KafkaConfig{}
	if err := config.UnmarshalKey(ConfigKafka, kfk); err != nil {
		panic(fmt.Errorf("Fatal error decoding Kafka config: %w \n", err))
	}

	// Build CRM configuration
	crm := &CRMConfig{}
	if err := config.UnmarshalKey(ConfigCrm, crm); err != nil {
		panic(fmt.Errorf("Fatal error decoding CRM config: %w \n", err))
	}

	// Build AM configuration
	am := &AMConfig{}
	if err := config.UnmarshalKey(ConfigAm, am); err != nil {
		panic(fmt.Errorf("Fatal error decoding AM config: %w \n", err))
	}

	// Build AM configuration
	air := &AirflowConfig{}
	if err := config.UnmarshalKey(ConfigAirflow, air); err != nil {
		panic(fmt.Errorf("Fatal error decoding Airflow config: %w \n", err))
	}

	// Build AM configuration
	deps := &SparkDependency{}
	if err := config.UnmarshalKey(ConfigSpark, deps); err != nil {
		panic(fmt.Errorf("Fatal error decoding Spark Dependencies config: %w \n", err))
	}
	for i, pyFile := range deps.PyFiles {
		if find := strings.Contains(pyFile, "%s"); find {
			deps.PyFiles[i] = fmt.Sprintf(pyFile, region)
		}
	}

	confs := &SparkConf{}
	err = json.Unmarshal([]byte(config.GetString(ConfigSparkConf)), &confs)
	if err != nil {
		panic(fmt.Errorf("Fatal error decoding Spark.conf config: %w \n", err))
	}

	executor := &Executor{}
	if err := config.UnmarshalKey(ConfigSparkExecutor, executor); err != nil {
		panic(fmt.Errorf("Fatal error decoding Spark.executor config: %w \n", err))
	}

	formattedExecutor := make(Executor)
	if len(*executor) > 0 {
		for _, v := range *executor {
			formattedExecutor[v.Name] = v
		}
	}

	spark := &SparkConfig{}
	spark.SparkConf = confs
	spark.SparkDependency = deps
	spark.Executor = &formattedExecutor

	configuration := &Configuration{
		S3:      s3,
		Kafka:   kfk,
		CRM:     crm,
		AM:      am,
		Ariflow: air,
		Spark:   spark,
		Env:     envWithNoRegion,
		Region:  region,
	}

	c, err := json.Marshal(configuration)
	fmt.Printf("Configuration %s\n\n\n", c)

	return configuration

}
