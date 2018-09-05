package mapreduce

import (
	"fmt"
	"sync"
)

type WorkerPool struct {
	sync.Mutex
	// protected by the mutex
	cond    *sync.Cond // signals when Register() adds to workers[]
	workers []string
}

// newMaster initializes a new Map/Reduce Master
func NewWorkerPool() (wp *WorkerPool) {
	wp = new(WorkerPool)
	wp.workers = make([]string, 0)
	wp.cond = sync.NewCond(wp)
	return wp
}

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	wpool := NewWorkerPool()

	// assign task
	var wg sync.WaitGroup

	// add new worker
	addWorker := func() {
		//defer wg.Done()
		for {
			select {
			case wname := <-registerChan:
				wpool.Lock()
				fmt.Printf("add worker %s to workpool\n", wname)
				wpool.workers = append(wpool.workers, wname)
				wpool.cond.Broadcast()
				wpool.Unlock()
			case <-registerChan: // for close channel signal
				return
			}
		}
	}

	runTask := func(worker string, taskArgs *DoTaskArgs) {
		call(worker, "Worker.DoTask", taskArgs, nil)
		wpool.Lock()
		wpool.workers = append(wpool.workers, worker)
		wpool.cond.Broadcast()
		wpool.Unlock()
	}

	// assign task goroutine
	assignTask := func() {
		defer wg.Done()
		//  this function take
		i := 0
		for i < ntasks {
			wpool.Lock()
			if len(wpool.workers) > 0 {
				for _, w := range wpool.workers {
					taskArgs := DoTaskArgs{JobName: jobName, File: mapFiles[i], Phase: phase,
						TaskNumber: i, NumOtherPhase: n_other}

					fmt.Printf("run task on worker %s\n", w)
					go runTask(w, &taskArgs)
					i++
					if i == ntasks {
						break
					}
				}
				wpool.workers = nil
			} else {
				wpool.cond.Wait()
			}
			wpool.Unlock()
		}
	}

	wg.Add(1)
	go addWorker()
	go assignTask()
	//fmt.Printf("Wait for task complete\n")
	wg.Wait()
	fmt.Printf("close Chan\n")
	close(registerChan)
	fmt.Printf("Schedule: %v phase done\n", phase)
}
