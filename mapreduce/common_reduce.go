package mapreduce

import (
	"os"
	"encoding/json"
	"log"
)


// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	//A string array of KEYVALUE PAIRS PRODUCED BY THE MAP FUNCTION
	reduceBuffer := make(map[string][]string)

	//for every map task 
	for i:= 0; i<nMap; i++ {

		//find the intermediate file for this reduce task from map task num  m using reduceNam! 
		//fileMapping --> all intermediate file produced 
		file := reduceName(jobName, i, reduceTaskNumber)
		fileMapping, err := os.Open(file)
		if err != nil {
			log.Fatal(err) } 
	
	//need to decode the encoded intermediate file 
	//"repeatedly calling .Decode() on it until Decode() returns an error."
	//from https://blog.golang.org/go-maps-in-action. Basic idea is the 
	dec := json.NewDecoder(fileMapping)

	//next few steps --> to sort the intermediate key & value pairs by keys 
	for { 
		var kv KeyValue 
		//opposite to what occured in common_map.go "err := enc.Encode(&kvPair) 
		err = dec.Decode(&kv) 
		if err != nil {
			break } 

	//if a keyValue has been decoded, add it to kvList!! 
	//Step 1: check if key already exists in the list. Step 2: if key does not exist
	//assign its value to empty!! Step 3: if key alreayd exists, add value to exitsting index
	////addapted from: http://stackoverflow.com/questions/2050391/how-to-check
        //if-a-map-contains-a-key-in-go

	if _, ok := reduceBuffer[kv.Key]; !ok {
		reduceBuffer[kv.Key] = []string{} 
	}else {
		reduceBuffer[kv.Key] = append(reduceBuffer[kv.Key], kv.Value)
	}
	}

	fileMapping.Close()

	//here adding all of the keys from the mapped intermediate file
        var totalKeys []string
        for k := range reduceBuffer {
                totalKeys = append(totalKeys, k)
        }

	//Since the list has all KeyValue pairs --> from map, now need to merge aka reduce 
	mergeFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil { 
		log.Fatal(err) } 

	//write the reduced output in as JSON encoded KeyValue objects to a file named mergeName
	enc := json.NewEncoder(mergeFile) 

	//Last step! --> loop over every key and for each call reduceF 
	for _,key := range totalKeys {
		r := reduceF(key, reduceBuffer[key])
		enc.Encode(KeyValue{key,r})
	}

	mergeFile.Close()
	
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
}
