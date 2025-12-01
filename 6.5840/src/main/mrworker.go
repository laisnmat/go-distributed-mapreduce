package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"os"
	"plugin"

	"6.5840/mr"
)

func main() {
	if len(os.Args) != 6 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker <worker_IP> <worker_Port> <coordinator_IP> <coordinator_Port> <plugin.so>\n")
		os.Exit(1)
	}

	workerIP := os.Args[1]
	workerPort := os.Args[2]
	coordinatorIP := os.Args[3]
	coordinatorPort := os.Args[4]
	pluginFile := os.Args[5]
	workerAddress := workerIP + ":" + workerPort
	coordinatorAddress := coordinatorIP + ":" + coordinatorPort
	mapf, reducef := loadPlugin(pluginFile)

	mr.Worker(workerAddress, coordinatorAddress, mapf, reducef)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
