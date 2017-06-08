package mapreduce

import (
	"fmt"
	"sync"
)
// schedule starts and waits for all tasks in the given phase (Map or Reduce).

//Used the following sources:
//https://tour.golang.org/concurrency/2
//https://www.goinggo.net/2014/01/concurrency-goroutines-and-gomaxprocs.html


func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ { 
	    wg.Add(1)
	    
	    //pulling each worker's information from the register 
	    args := new(DoTaskArgs)
            args.JobName = mr.jobName
	    args.Phase = phase
	    args.NumOtherPhase = nios
	    args.File = mr.files[i]
	    args.TaskNumber = i

	    go func() {
		defer wg.Done()

	  	// if the master's RPC to the worker fails, the master
                // re-assigns the task given of the failed worker to another worker.
		for {

		    //i = current task. For each task, a new worker is assigned
		    worker := <-mr.registerChannel

		    // call() sends an RPC to the rpcname handler on server srv
		    // with arguments args, waits for the reply, and leaves the
		   // reply in reply. the reply argument should be the address
		   // of a reply structure.

		   //if ok = true, make create a new thread, perform the task
		   //and then return the worker back to the register to 
		   //to signal that he is availble for future tasks and then break 
		   
		   //if ok = false then the worker fails, and we loop again
                    //until the task is successfully completed

		    ok := call(worker, "Worker.DoTask", args, new(struct{}))
		   
		    if ok {
			go func() {
			    mr.registerChannel <-worker
			}()
			break
		    }else {
			debug("ERROR IN:", worker, mr.jobName, i)
		    }
                  }
	    }()
	 }
	 wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
