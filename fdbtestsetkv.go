package main

//#include "/root/forestdb-2ibenchmark/strgen.h"
import (
        "C"
        "github.com/couchbase/goforestdb"
        "time"
        "fmt"
        "math/rand"
)

var totaldocs = 0
var str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!"
var charset = []byte(str)
var lenc = len(charset)-1

func strgen(bufferslice []byte, strlen int) {

   for n := 0; n < strlen; n++ {
             key := rand.Intn(1000000) % lenc
             bufferslice[n] = charset[key]
            }

}
 
func fdb_init_load() {

// Open a database
   config := forestdb.DefaultConfig()
   config.SetBufferCacheSize(10737418240)
   config.SetDurabilityOpt(forestdb.DRB_ASYNC)
   config.SetCompactionMode(forestdb.COMPACT_MANUAL)
   db, _ := forestdb.Open("secondaryDB", config)
   kvconfig := forestdb.DefaultKVStoreConfig()
   kvstore1,_ := db.OpenKVStore("kvstore_1", kvconfig)
   kvstore2,_ := db.OpenKVStore("kvstore_2", kvconfig)
   numdocswritten := 0
   keylen := 40
   fmt.Printf("key length %d\n", keylen)
   mybufferslice1 := make([]byte, keylen)
   mybufferslice2 := make([]byte, keylen)
// Close it properly when we're done
   defer db.Close()
   defer kvstore1.Close()
   defer kvstore2.Close()
   start := time.Now().Unix()  
 
   for (numdocswritten < totaldocs) {
   	strgen(mybufferslice1, keylen)
   	strgen(mybufferslice2, keylen)
	// Store the document
   	kvstore1.SetKV(mybufferslice1, nil)
        numdocswritten++
   	kvstore2.SetKV(mybufferslice2, nil)
        numdocswritten++
        if((time.Now().Unix()-start)>60){
            db.Commit(forestdb.COMMIT_NORMAL)
            start = time.Now().Unix();
       }
   }
   db.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)

}

func main() {
   totaldocs = 100000000
   rand.Seed(time.Now().UnixNano())
   index_starttime := time.Now().Unix()
   fdb_init_load()
   index_stoptime := time.Now().Unix()
   fmt.Printf("Index build time: ")
   fmt.Println(index_stoptime-index_starttime)
}
