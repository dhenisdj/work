package models

import (
	"math/rand"
)

type PrioritySampler struct {
	sum     uint
	Samples []sampleItem
}

type sampleItem struct {
	priority uint

	// payload:
	RedisJobs               string
	RedisJobsInProg         string
	RedisJobsPaused         string
	RedisJobsLock           string
	RedisJobsLockInfo       string
	RedisJobsMaxConcurrency string
}

func (s *PrioritySampler) Add(priority uint, redisJobs, redisJobsInProg, redisJobsPaused, redisJobsLock, redisJobsLockInfo, redisJobsMaxConcurrency string) {
	sample := sampleItem{
		priority:                priority,
		RedisJobs:               redisJobs,
		RedisJobsInProg:         redisJobsInProg,
		RedisJobsPaused:         redisJobsPaused,
		RedisJobsLock:           redisJobsLock,
		RedisJobsLockInfo:       redisJobsLockInfo,
		RedisJobsMaxConcurrency: redisJobsMaxConcurrency,
	}
	s.Samples = append(s.Samples, sample)
	s.sum += priority
}

// Sample re-sorts s.samples, modifying it in-place. Higher weighted things will tend to go towards the beginning.
// NOTE: as written currently makes 0 allocations.
// NOTE2: this is an O(n^2 algorithm) that is:
//     5492ns for 50 jobs (50 is a large number of unique jobs in my experience)
//     54966ns for 200 jobs
//     ~1ms for 1000 jobs
//     ~4ms for 2000 jobs
func (s *PrioritySampler) Sample() []sampleItem {
	lenSamples := len(s.Samples)
	remaining := lenSamples
	sumRemaining := s.sum
	lastValidIdx := 0

	// Algorithm is as follows:
	// Loop until we sort everything. We're going to sort it in-place, probabilistically moving the highest weights to the front of the slice.
	//   Pick a random number
	//   Move backwards through the slice on each iteration,
	//     and see where the random number fits in the continuum.
	//     If we find where it fits, sort the item to the next slot towards the front of the slice.
	for remaining > 1 {
		// rn from [0 to sumRemaining)
		rn := uint(rand.Uint32()) % sumRemaining

		prevSum := uint(0)
		for i := lenSamples - 1; i >= lastValidIdx; i-- {
			sample := s.Samples[i]
			if rn < (sample.priority + prevSum) {
				// move the sample to the beginning
				s.Samples[i], s.Samples[lastValidIdx] = s.Samples[lastValidIdx], s.Samples[i]

				sumRemaining -= sample.priority
				break
			} else {
				prevSum += sample.priority
			}
		}

		lastValidIdx++
		remaining--
	}

	return s.Samples
}
