package mapreduce

import (
	"os"
	"log"
	"encoding/json"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	// 
	kvMap := make(map[string][]string)

	// decode the file and load the KeyValue into map 
	for i:=0; i<nMap; i++ {
		fname := reduceName(jobName, i, reduceTaskNumber)
		f, err := os.Open(fname)
		defer f.Close()
		if err != nil {
			log.Fatalf("Failed to Read File: %s\n", fname)
		}

		dec := json.NewDecoder(f)

		var kv KeyValue
		for dec.More() {
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatalf("Failed to Decode file: %s\n", fname)
			}

			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	// Open output file for writing
	fout, err := os.Create(outFile)
	defer fout.Close()
	if err != nil {
		log.Fatalf("Failed to create output File: %s\n", outFile)
	}
	enc := json.NewEncoder(fout)

	// call reduce function to reduce the value
	for k, values := range kvMap {
		v := reduceF(k, values)
		enc.Encode(&KeyValue{k, v})
	}
}
