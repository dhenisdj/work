package work

import (
	"fmt"
	"math/rand"
	"time"
)

func epsilonHandler(job *Job) error {
	fmt.Println("epsilon")
	time.Sleep(time.Second)

	if rand.Intn(2) == 0 {
		return fmt.Errorf("random error")
	}
	return nil
}
