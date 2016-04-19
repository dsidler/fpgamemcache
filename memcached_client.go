package main

import (
   "github.com/dsidler/fpgamemcache/memcache"
   "fmt"
   "os"
   "sync"
   "time"
   "math/rand"
   "flag"
   //"runtime/pprof"
   //"log"
)

type configuration struct {
   host        string
   numClients  int 
   numRuns     int
   setProb     float64
   valueLength int
}
const letters = "0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz"

func GenerateKeys(numKeys int) []string {
   // Generate base key
   key := make([]byte, 16)
   for i := 0; i < 16; i++ {
      key[i] = letters[i % len(letters)]
   }
   keys := make([]string, numKeys)
   temp := make([]byte, 16)
   // Gnerate 1K keys, in range
   pre := 0
   for i := 0; i < numKeys; i++ {
      for j := 0; j < 12; j++ {
         temp[j] = key[j];
      }
      for j := 12; j < 14; j++ {
         temp[j] = letters[0];
      }
      temp[14] = letters[pre % len(letters)];
      temp[15] = letters[i % len(letters)];
      if (i % len(letters)) == 0 {
         pre++;
      }
      keys[i] = string(temp)
   }
   return keys
}

//TODO Generate Skewed Keys

func client(wg * sync.WaitGroup, s chan bool, mc *memcache.Client, config *configuration) {
   defer wg.Done()
   numKeys := 1000
   oracle := rand.New(rand.NewSource(time.Now().UnixNano()))

   // Generate keys
   keys := GenerateKeys(numKeys)
   // Generate value
   value := make([]byte, config.valueLength)
   for i := 0; i < config.valueLength; i++ {
      value[i] = letters[i % len(letters)]
   }

   // Wait for start signal
   startsig := <-s
   if !startsig {
      fmt.Println("Wrong start signal.")
   }

   runs := 0
   for ; runs < config.numRuns; runs++ {
      prob := oracle.Float64()
      //if runs % 10 == 0 {
      if prob < config.setProb {
         //key := "foobarbafoobarba"
         //value := []byte("0123456789012345678901234567890123456789012345678901")
         err := mc.SetJSON(&memcache.Item{Key: keys[runs % numKeys], Value: value})
         if err != nil {
            fmt.Println("Error on set: ", err.Error())
            os.Exit(1)
         }
      } else {
         key := "foobarbafoobarba"
         //value := []byte("0123456789abcdef0123456789abcdef")
         _, err := mc.Get(key)
         //_, err := mc.Ret(&memcache.Item{Key: key, Value: value})
         if err != nil {
            fmt.Println("Error on get: ", err.Error())
            os.Exit(1)

         }
      }

   }
}

//var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")


func main() {
   //runtime.GOMAXPROCS(8)
   hostPtr := flag.String("host", "10.1.212.209:2888", "host addr and port")
   numPtr := flag.Int("clients,c", 1, "number of clients")
   runPtr := flag.Int("runs,r", 1, "number of runs per client")
   setPtr := flag.Float64("setp,s", 0.1, "probability of set")
   valuePtr := flag.Int("vallen,v", 32, "length of value")
   flag.Parse()

   /*if *cpuprofile != "" {
     f, err := os.Create(*cpuprofile)
     if err != nil {
        log.Fatal(err)
     }
     pprof.StartCPUProfile(f)
     defer pprof.StopCPUProfile()
   }*/

   config := configuration {
               host: *hostPtr,
               numClients: *numPtr,
               numRuns: *runPtr,
               setProb: *setPtr,
               valueLength: *valuePtr }
   if config.valueLength < 32 {
      fmt.Println("value length too short, must be at least 32")
      os.Exit(1)
   }

   //TODO make host as input
   mc := memcache.New(config.host)
   // Set max idle cons
   mc.MaxIdleConns = config.numClients+10//(numClients/2)
   // set network timeout
   mc.Timeout = 5000 * time.Millisecond

   wg := new(sync.WaitGroup)
   start := make(chan bool)

   // start clients
   for i := 0; i < config.numClients; i++ {
      wg.Add(1)
      go client(wg, start, mc, &config)
   }

   // wait for clients to setup
   time.Sleep(time.Second*3)

   fmt.Println("Start...")
   starttime := time.Now()
   for i := 0; i < config.numClients; i++ {
      start <- true
   }
   wg.Wait()
   duration := time.Since(starttime).Seconds()

   numReqs := config.numRuns*config.numClients
   fmt.Printf("Throughput[KReq/s]: %2f\n", float64(numReqs) / duration / 1000)
   fmt.Printf("Duration[ms]: %2f\n", duration*1000)

}
