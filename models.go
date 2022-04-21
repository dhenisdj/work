package work

type Response struct {
	Code     int      `json:"code"`
	Message  string   `json:"msg"`
	Task     BaseTask `json:"data"`
	Sequence string   `json:"sequence"`
	Cost     int      `json:"cost"`
}

type BaseTask struct {
	Id            int64  `json:"id"`
	Executor      string `json:"executor"`    // executor function
	Context       string `json:"context"`     // executor params
	TaskStatus    int32  `json:"task_status"` // 1：waiting，2：doing，3：done，4：fail
	RefType       string `json:"ref_type"`    // ref type, specific table name
	RefID         int64  `json:"ref_id"`      // table id associated with the task
	BusinessGroup string `json:"business_group"`
	Region        string `json:"region"`
	Env           string `json:"env"`
}

// Configuration structs

type S3Config struct {
	Host      string `json:"host"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	Bucket    string `json:"bucket"`
}

type KafkaConfig struct {
	Brokers   []string `json:"brokers"`
	Topic     string   `json:"topic"`
	MetaTopic string   `json:"metaTopic"`
}

type Credential struct {
	Key    string `json:"key"`
	Secret string `json:"secret"`
}

type CRMConfig struct {
	Credential *Credential       `json:"credential"`
	Callback   map[string]string `json:"callback"`
}

type AMConfig struct {
	Credential *Credential       `json:"credential"`
	Callback   map[string]string `json:"callback"`
	Schema     map[string]string `json:"schema"`
	Hdfs       map[string]string `json:"hdfs"`
}

type AirflowConfig struct {
	Seatalk string `json:"seatalk"`
}

type Account struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

type Business struct {
	Validate    []string           `json:"validate"`
	Account     map[string]Account `json:"account"`
	Concurrency map[string]int     `json:"concurrency"`
	Queue       map[string]string  `json:"queue"`
}

func (b *Business) validate() bool {
	f := false
	for biz := range b.Concurrency {
		f, _ = Contain(b.Validate, biz)
		if !f {
			f = false
		}
		continue
	}
	return f
}

func (b *Business) validateBusiness(biz string) bool {
	f, err := Contain(b.Validate, biz)
	if err != nil {
		return false
	}
	return f
}

type SparkResource struct {
	DriverMemory   string `json:"driverMemory"`
	DriverCores    int    `json:"driverCores"`
	ExecutorMemory string `json:"executorMemory"`
	ExecutorCores  int    `json:"executorCores"`
	NumExecutor    int    `json:"numExecutor"`
}

type BusinessSparkResource struct {
	Name     string         `json:"name"`
	Type     string         `json:"type"`
	Business *Business      `json:"business"`
	Resource *SparkResource `json:"resource"`
}

type Executor map[string]*BusinessSparkResource

type SparkDependency struct {
	Jars     []string `json:"jars"`
	PyFiles  []string `json:"pyFiles"`
	Archives []string `json:"archives"`
}

type SparkConf map[string]string

type SparkConfig struct {
	*SparkDependency
	*SparkConf
	*Executor
}

type Configuration struct {
	S3      *S3Config      `json:"s3"`
	Kafka   *KafkaConfig   `json:"kafka"`
	CRM     *CRMConfig     `json:"crm"`
	AM      *AMConfig      `json:"am"`
	Ariflow *AirflowConfig `json:"ariflow"`
	Spark   *SparkConfig   `json:"spark"`
	Env     string         `json:"env"`
	Region  string         `json:"region"`
}
