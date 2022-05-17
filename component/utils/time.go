package utils

import "time"

var nowMock int64

func NowEpochSeconds() int64 {
	if nowMock != 0 {
		return nowMock
	}
	return time.Now().Unix()
}

func SetNowEpochSecondsMock(t int64) {
	nowMock = t
}

func ResetNowEpochSecondsMock() {
	nowMock = 0
}

// EpochSecondsToTime convert epoch seconds to a time
func EpochSecondsToTime(t int64) time.Time {
	return time.Time{}
}
