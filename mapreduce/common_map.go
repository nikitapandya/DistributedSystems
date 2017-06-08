package mapreduce

import (
	"hash/fnv"
        "log"
        "os"
        "encoding/json"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	//First open file --> to read:
	//https://golang.org/pkg/os/
        file,err := os.Open(inFile)
	if err != nil {
		log.Fatal(err) 
        }
	
	//Stat = returns contents of file 
	info ,err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	//to get the size of the file --> to know how large the array "contents" should be 
	size := info.Size()
	contents := make( []byte, size)
	
	//the file's data can then be read into a slice of bytes. Read and Write take their byte counts from the length of the argument slice.
	//underscore --> a blank identifier in order to discard the first returned value --> becuase not using count anywhere else 
	//https://golang.org/pkg/os/
	_,err = file.Read(contents)
	if err != nil { 
		log.Fatal(err)
	}

	file.Close()

	//START MAPPING --> contents array contains all data 

	kvMap := mapF(inFile, string(contents))
	//hash (ihash) and map (reduceName) data to prepare the KeyValue list (KeyValue struct --> Key,Value)

	//for every reduce task 
	for i:= 0; i<nReduce; i++ {
		
		//the intermediate output of a map taks is stored in the file system whose name indicates which map tasks produced them
		//as well as which reduce task they are for --> reduceName 
		//reduceName constructs the name of the intermediate file which map task --> return type string
		tempFile, err := os.Create(reduceName(jobName, mapTaskNumber, i))
		if err != nil {
			log.Fatal(err)
		}
		
		//but as the output of the reduce tasks *must* be JSON
		enc := json.NewEncoder(tempFile)
		
		for _, kvPair := range kvMap {
	
			//ihash --> to decide which file a given key belongs into
			//hash each pair's key!!! 
			//mod the number returned from ihash() with nReduce (to remain in nReduce's range) 
			//and if it is equal to the current reduce file number (i), add the kvPair

			if ihash(kvPair.Key) % uint32(nReduce) == uint32(i) {
				err := enc.Encode(&kvPair)
				if err != nil {
					log.Fatal(err)	}
			}
		}
		tempFile.Close()
	}
// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
