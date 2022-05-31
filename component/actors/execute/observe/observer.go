package observe

import (
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/component/utils"
	"time"
)

// An Observer observes a single work. Each work has its own observer.
type Observer struct {
	namespace string
	ctx       context.Context
	workerID  string

	// nil: worker isn't doing anything that we know of
	// not nil: the last started observation that we received on the channel.
	// if we get an checkin, we'll just update the existing observation
	currentStartedObservation *observation

	// version of the data that we wrote to redis.
	// each observation we get, we'll update version. When we flush it to redis, we'll update lastWrittenVersion.
	// This will keep us from writing to redis unless necessary
	version, lastWrittenVersion int64

	observationsChan chan *observation

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

type observationKind int

const (
	observationKindStarted observationKind = iota
	observationKindDone
	observationKindCheckin
)

type observation struct {
	kind observationKind

	// These fields always need to be set
	jobName string
	jobID   string

	// These need to be set when starting a task
	startedAt int64
	arguments map[string]interface{}

	// If we're done w/ the task, err will indicate the success/failure of it
	err error // nil: success. not nil: the error we got when running the task

	// If this is a checkin, set these.
	checkin   string
	checkinAt int64
}

const observerBufferSize = 1024

func NewObserver(ctx context.Context, namespace string, workerID string) *Observer {
	return &Observer{
		namespace:        namespace,
		ctx:              ctx,
		workerID:         workerID,
		observationsChan: make(chan *observation, observerBufferSize),

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}
}

func (o *Observer) Start() {
	go o.loop()
}

func (o *Observer) Stop() {
	o.stopChan <- struct{}{}
	<-o.doneStoppingChan
}

func (o *Observer) Drain() {
	o.drainChan <- struct{}{}
	<-o.doneDrainingChan
}

func (o *Observer) ObserveStarted(jobName, jobID string, arguments map[string]interface{}) {
	o.observationsChan <- &observation{
		kind:      observationKindStarted,
		jobName:   jobName,
		jobID:     jobID,
		startedAt: utils.NowEpochSeconds(),
		arguments: arguments,
	}
}

func (o *Observer) ObserveDone(jobName, jobID string, err error) {
	o.observationsChan <- &observation{
		kind:    observationKindDone,
		jobName: jobName,
		jobID:   jobID,
		err:     err,
	}
}

func (o *Observer) ObserveCheckin(jobName, jobID, checkin string) {
	o.observationsChan <- &observation{
		kind:      observationKindCheckin,
		jobName:   jobName,
		jobID:     jobID,
		checkin:   checkin,
		checkinAt: utils.NowEpochSeconds(),
	}
}

func (o *Observer) loop() {
	// Every tick we'll update redi if necessary
	// We don't update it on every task because the only purpose of this data is for humans to inspect the system,
	// and a fast work could move onto new jobs every few ms.
	ticker := time.Tick(1000 * time.Millisecond)

	for {
		select {
		case <-o.stopChan:
			o.doneStoppingChan <- struct{}{}
			return
		case <-o.drainChan:
		DRAIN_LOOP:
			for {
				select {
				case obv := <-o.observationsChan:
					o.process(obv)
				default:
					if err := o.writeStatus(o.currentStartedObservation); err != nil {
						o.ctx.LE("observe.write", err)
					}
					o.doneDrainingChan <- struct{}{}
					break DRAIN_LOOP
				}
			}
		case <-ticker:
			if o.lastWrittenVersion != o.version {
				if err := o.writeStatus(o.currentStartedObservation); err != nil {
					o.ctx.LE("observe.write", err)
				}
				o.lastWrittenVersion = o.version
			}
		case obv := <-o.observationsChan:
			o.process(obv)
		}
	}
}

func (o *Observer) process(obv *observation) {
	if obv.kind == observationKindStarted {
		o.currentStartedObservation = obv
	} else if obv.kind == observationKindDone {
		o.currentStartedObservation = nil
	} else if obv.kind == observationKindCheckin {
		if (o.currentStartedObservation != nil) && (obv.jobID == o.currentStartedObservation.jobID) {
			o.currentStartedObservation.checkin = obv.checkin
			o.currentStartedObservation.checkinAt = obv.checkinAt
		} else {
			o.ctx.LE("observe.checkin_mismatch", fmt.Errorf("got checkin but mismatch on task ID or no task"))
		}
	}
	o.version++

	// If this is the version observation we got, just go ahead and write it.
	if o.version == 1 {
		if err := o.writeStatus(o.currentStartedObservation); err != nil {
			o.ctx.LE("observe.first_write", err)
		}
		o.lastWrittenVersion = o.version
	}
}

func (o *Observer) writeStatus(obv *observation) error {
	conn := o.ctx.Redis().Get()
	defer conn.Close()

	key := models.RedisKey2Observation(o.namespace, o.workerID)

	if obv == nil {
		if _, err := conn.Do("DEL", key); err != nil {
			return err
		}
	} else {
		// hash:
		// job_name -> obv.Name
		// job_id -> obv.jobID
		// started_at -> obv.startedAt
		// args -> json.Encode(obv.arguments)
		// checkin -> obv.checkin
		// checkin_at -> obv.checkinAt

		var argsJSON []byte
		if len(obv.arguments) == 0 {
			argsJSON = []byte("")
		} else {
			var err error
			argsJSON, err = json.Marshal(obv.arguments)
			if err != nil {
				return err
			}
		}

		args := make([]interface{}, 0, 13)
		args = append(args,
			key,
			"job_name", obv.jobName,
			"job_id", obv.jobID,
			"started_at", obv.startedAt,
			"status", obv.kind,
			"args", argsJSON,
		)

		if (obv.checkin != "") && (obv.checkinAt > 0) {
			args = append(args,
				"checkin", obv.checkin,
				"checkin_at", obv.checkinAt,
			)
		}

		conn.Send("HMSET", args...)
		conn.Send("EXPIRE", key, 60*60*24)
		if err := conn.Flush(); err != nil {
			return err
		}

	}

	return nil
}
