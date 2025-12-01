package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.5840/mr"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator <coordinator_IP> <coordinator_PORT> <inputfiles...>\n")
		os.Exit(1)
	}

	coordinatorIP := os.Args[1]
	coordinatorPort := os.Args[2]
	inputFiles := os.Args[3:]
	address := coordinatorIP + ":" + coordinatorPort

	m := mr.MakeCoordinator(address, inputFiles, 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
