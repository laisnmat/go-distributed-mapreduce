# go-distributed-mapreduce

![Go](https://img.shields.io/badge/Go-00ADD8?logo=go&logoColor=white)
![Distributed Systems](https://img.shields.io/badge/topic-distributed%20systems-blue)

A **fault-tolerant distributed MapReduce** system in Go, inspired by Google's [MapReduce paper](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/). A coordinator hands out Map and Reduce tasks to workers over RPC, workers run in parallel on one or several machines, and the job completes correctly even when workers crash.

*Chalmers University of Technology – 2025 | Team of 2*

---

## Table of contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Fault tolerance](#fault-tolerance)
- [Project structure](#project-structure)
- [Getting started](#getting-started)
- [Running a job on one machine](#running-a-job-on-one-machine)
- [Running across several machines](#running-across-several-machines)
- [Testing fault tolerance](#testing-fault-tolerance)
- [Design choices](#design-choices)
- [References](#references)
- [Acknowledgments](#acknowledgments)

---

## Overview

MapReduce is a programming model for processing large datasets in parallel. A job is split into two phases:

1. **Map**: each input file is processed independently and turned into a list of intermediate `(key, value)` pairs.
2. **Reduce**: all values sharing the same key are grouped together and combined into a final result.

The classic example is **word count**: Map emits `(word, "1")` for every word, and Reduce sums the counts for each word.

The user only writes the `Map` and `Reduce` functions. This project provides everything else: splitting the work into tasks, scheduling them on workers, moving intermediate data between machines, and recovering from failures.

## Features

- **Coordinator / worker architecture** communicating over **RPC on TCP**
- **Parallel execution** of Map and Reduce tasks across any number of workers
- **Pluggable applications**: Map and Reduce functions loaded at runtime as Go plugins (word count, inverted index, and more)
- **Multi-machine deployment without a shared file system**: intermediate files stay on the worker that produced them and are fetched over RPC
- **Worker failure detection** with heartbeats and task timeouts
- **Automatic re-execution** of tasks lost with a failed worker, including completed Map tasks whose output is no longer reachable
- **Coordinator failure detection** on the worker side
- **Crash-safe outputs**: files are written to a temporary location and moved into place only once complete

## Architecture

```
                         ┌──────────────────────┐
                         │     Coordinator      │
                         │  task scheduling,    │
                         │  heartbeats, state   │
                         └──────────┬───────────┘
                    GetTask / SetTaskDone / SetTaskFailed (RPC)
                                    │   Heartbeat (RPC)
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
       ┌────────────┐        ┌────────────┐        ┌────────────┐
       │  Worker 1  │◄──────►│  Worker 2  │◄──────►│  Worker 3  │
       │ mr-X-Y ... │        │ mr-X-Y ... │        │ mr-X-Y ... │
       └────────────┘        └────────────┘        └────────────┘
            GetIntermediate (RPC): reducers fetch intermediate
            files directly from the mappers that produced them
```

### Coordinator

The coordinator keeps the state of every task (`Idle`, `InProgress` or `Completed`) and answers workers' requests:

- **`GetTask`** gives a worker an idle Map task, or, once all Map tasks are done, an idle Reduce task. If nothing is available yet it replies "wait", and once the whole job is finished it replies "exit".
- **`SetTaskDone`** marks a task as completed and, for Map tasks, records **which worker holds its output**.
- **`SetTaskFailed`** resets a failed task so it can be reassigned.

### Worker

Each worker runs its own RPC server and loops on `GetTask`:

- **Map task**: reads its input file, calls the application's `Map` function, partitions the pairs into `nReduce` buckets with `ihash(key) % nReduce`, and writes one JSON file per bucket, named `mr-<mapId>-<reduceId>`, **on its own disk**.
- **Reduce task**: receives from the coordinator the address of the worker holding each Map output, fetches its bucket from every mapper with the `GetIntermediate` RPC, sorts the pairs by key, calls the application's `Reduce` function for each key, and writes the result to `mr-out-<reduceId>`.

## Fault tolerance

| Failure | How it is detected | How it is handled |
|---------|-------------------|-------------------|
| A worker crashes | The coordinator sends a heartbeat to every worker every 2 s; after 3 missed heartbeats the worker is declared dead | All its tasks are reset to `Idle`, **including completed Map tasks**, since their output lived on the dead worker's disk |
| A worker is stuck or too slow | A task has been `InProgress` for more than 10 s | The task is handed to the next worker that asks for one |
| A reducer cannot reach a mapper | `GetIntermediate` fails after retries | The reducer reports the unreachable Map tasks with `SetTaskFailed`; the coordinator re-runs them, then the Reduce task is retried |
| The coordinator crashes | A worker's RPC to the coordinator fails after retries | The worker detects it and shuts down cleanly |
| A crash in the middle of writing a file | – | Outputs are written to a temporary file and moved to their final name only once complete, so a partial file is never visible |

## Project structure

The repository contains the MIT 6.5840 lab framework, with the MapReduce implementation in `src/mr/`:

```
6.5840/src/
├── mr/
│   ├── coordinator.go   # Task scheduling, worker heartbeats, failure handling
│   ├── worker.go        # Map/Reduce execution, intermediate file server, RPC client
│   ├── rpc.go           # RPC argument and reply types
│   └── movefile.go      # Cross-device safe file move for atomic outputs
├── main/
│   ├── mrcoordinator.go # Coordinator entry point
│   ├── mrworker.go      # Worker entry point
│   ├── mrsequential.go  # Sequential reference implementation (provided)
│   └── pg-*.txt         # Sample input files (books from Project Gutenberg)
└── mrapps/              # MapReduce applications built as plugins (provided)
    ├── wc.go            # Word count
    ├── indexer.go       # Inverted index
    └── crash.go         # Word count that randomly crashes, for fault tolerance tests
```

## Getting started

### Prerequisites

- [Go](https://go.dev/dl/) 1.15 or later
- **Linux or macOS**: workers load applications with Go's `plugin` package, which does not support Windows. On Windows, use [WSL](https://learn.microsoft.com/windows/wsl/install).

### Build

```bash
git clone https://github.com/laisnmat/go-distributed-mapreduce.git
cd go-distributed-mapreduce/6.5840/src/main

go build -buildmode=plugin ../mrapps/wc.go
go build mrcoordinator.go
go build mrworker.go
```

- `go build -buildmode=plugin ../mrapps/wc.go` compiles the word count application into `wc.so`, a shared library that workers load at runtime. Build `indexer.go` or any other app the same way.
- `go build mrcoordinator.go` and `go build mrworker.go` produce the `mrcoordinator` and `mrworker` executables.

> A plugin must be rebuilt whenever the code in `mr/` changes, otherwise the worker refuses to load it.

### Command-line usage

```bash
./mrcoordinator <coordinator_ip> <coordinator_port> <input_files...>
./mrworker <worker_ip> <worker_port> <coordinator_ip> <coordinator_port> <plugin.so>
```

| Argument | Description |
|----------|-------------|
| `coordinator_ip`, `coordinator_port` | Address the coordinator listens on, and that workers connect to |
| `input_files...` | Input files; each one becomes one Map task |
| `worker_ip`, `worker_port` | Address the worker listens on; the coordinator uses it for heartbeats and other workers use it to fetch intermediate files |
| `plugin.so` | Compiled MapReduce application |

The job uses **10 Reduce tasks**, so it produces 10 output files, `mr-out-0` to `mr-out-9`.

## Running a job on one machine

All commands are run from `6.5840/src/main`.

**1. Clean previous outputs**

```bash
rm -f mr-out* mr-*-*
```

**2. Start the coordinator** on port 9000, with all sample books as input (8 Map tasks):

```bash
./mrcoordinator 127.0.0.1 9000 pg*.txt
```

**3. Start several workers**, each in its own terminal and on its own port:

```bash
./mrworker 127.0.0.1 9001 127.0.0.1 9000 wc.so
```
```bash
./mrworker 127.0.0.1 9002 127.0.0.1 9000 wc.so
```
```bash
./mrworker 127.0.0.1 9003 127.0.0.1 9000 wc.so
```

Workers pick up tasks as soon as they start. When the job is finished, the coordinator and the workers exit on their own.

**4. Look at the result**

```bash
cat mr-out-* | sort | head
```

```
A 509
ABOUT 2
ACT 8
...
```

`cat mr-out-*` concatenates the 10 output files and `sort` orders the words alphabetically.

**5. Check it against the sequential version**

```bash
cat mr-out-* | sort > distributed.txt
go run mrsequential.go wc.so pg*.txt
sort mr-out-0 > sequential.txt
cmp distributed.txt sequential.txt && echo "Results match"
```

`mrsequential.go` runs the same job in a single process. `cmp` prints nothing and returns success when both files are identical.

> Run step 5 last: `mrsequential.go` writes its own `mr-out-0`, which overwrites the distributed one.

## Running across several machines

No shared file system is needed: each worker keeps its intermediate files locally and serves them to reducers over RPC.

**On every machine**, clone the repository and build the binaries and the plugin as shown in [Build](#build). Each machine then has its own copy of the input files at the same path.

**Make sure the ports are reachable**: the coordinator's port must be open to all workers, and each worker's port must be open to the coordinator and to the other workers (for example, allow them in the security group of your cloud VMs).

**On the coordinator machine:**

```bash
./mrcoordinator <coordinator_ip> 9000 pg*.txt
```

**On each worker machine:**

```bash
./mrworker <this_machine_ip> 9001 <coordinator_ip> 9000 wc.so
```

Use IP addresses that the other machines can reach (for example private IPs inside the same cloud network), never `127.0.0.1`.

Each `mr-out-X` file is written on the machine of the worker that ran that Reduce task. To gather the full result, copy them to a single machine, for example:

```bash
scp user@<worker_ip>:go-distributed-mapreduce/6.5840/src/main/mr-out-* .
```

## Testing fault tolerance

The `crash` application behaves like word count but randomly makes workers crash or hang during Map and Reduce. It is a good way to see failure recovery in action.

**1. Build the plugins**

```bash
go build -buildmode=plugin ../mrapps/crash.go
go build -buildmode=plugin ../mrapps/nocrash.go
```

**2. Start the coordinator**

```bash
rm -f mr-out* mr-*-*
./mrcoordinator 127.0.0.1 9000 pg*.txt
```

**3. Start workers that restart automatically after each crash**

```bash
p=9101; while true; do ./mrworker 127.0.0.1 $p 127.0.0.1 9000 crash.so; p=$((p+1)); done
```

This loop starts a worker and, whenever it crashes or exits, starts a new one on the next port. Run the same loop in a second terminal starting at another port (for example `p=9201`). Stop the loops with `Ctrl+C` once the coordinator has exited.

In the coordinator's output, lines like `worker 127.0.0.1:9104 failed (no heartbeat). Resetting its tasks.` show failures being detected and tasks being reassigned.

**4. Check the result**

```bash
cat mr-out-* | sort > crash.txt
go run mrsequential.go nocrash.so pg*.txt
sort mr-out-0 > expected.txt
cmp crash.txt expected.txt && echo "Results match despite crashes"
```

`nocrash.so` computes the same result as `crash.so` without crashing, so it gives the expected output.

## Design choices

- **Pull-based scheduling**: workers ask for tasks instead of the coordinator pushing them, so a new worker can join at any time and an idle worker simply asks again.
- **Intermediate data stays local**: as in the original paper, Map outputs are never copied to a central place. Reducers fetch them directly from the mappers, which removes the need for a shared file system.
- **Two complementary failure detectors**: heartbeats detect workers that are dead, while the 10 s task timeout catches workers that are alive but stuck.
- **Re-running completed Map tasks** of a dead worker, because a completed task is useless if its output can no longer be read.
- **Reducer-reported failures**: a reducer that cannot reach a mapper tells the coordinator exactly which Map tasks to redo, instead of waiting for the heartbeat to notice.
- **Temporary file + move** for every output, so a crash never leaves a half-written file behind. A copy-and-delete move is used instead of `os.Rename`, which fails when the temporary directory is on a different partition (`invalid cross-device link`), as can happen on remote machines.
- **One mutex** protecting the coordinator's state, released before making outgoing RPCs such as heartbeats.

## References

- Jeffrey Dean and Sanjay Ghemawat, [*MapReduce: Simplified Data Processing on Large Clusters*](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/), OSDI 2004.

## Acknowledgments

Lab framework, sample applications and input files from **MIT 6.5840 (Distributed Systems)**, used in the Distributed Systems course at Chalmers University of Technology.
