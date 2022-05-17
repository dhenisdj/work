package main

import (
	"fmt"
	"time"
)

func main() {

	timer1 := time.NewTimer(1 * time.Second)
	timer2 := time.NewTimer(5 * time.Second)

	for {
		select {
		case <-timer1.C:
			fmt.Println("process job")
			timer1.Reset(1 * time.Second)
			select {
			case <-timer2.C:
				fmt.Println("the interactive session renewing")
				timer2.Reset(5 * time.Second)
			default:
				fmt.Println("session not reach limit")
			}
		}
	}

}
