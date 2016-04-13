package main

import (
   "github.com/dsidler/fpgamemcache/memcache"
   "fmt"
   "os"
   "sync"
   "time"
   //"math/rand"
   "flag"
   //"runtime/pprof"
   //"log"
)


func client(wg * sync.WaitGroup, s chan bool, mc *memcache.Client, numRuns int, setProb float64) {
   defer wg.Done()
   //oracle := rand.New(rand.NewSource(time.Now().UnixNano()))

   // Wait for start signal
   startsig := <-s
   if !startsig {
      fmt.Println("Wrong start signal.")
   }

   runs := 0
   for ; runs < numRuns; runs++ {
      //prob := oracle.Float64()
      if runs % 10 == 0 {//prob < setProb {
         key := "foobarbafoobarba"
         value := []byte("0123456789012345678901234567890123456789012345678901")
         err := mc.SetJSON(&memcache.Item{Key: key, Value: value})
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
   numPtr := flag.Int("clients", 1, "number of clients")
   runPtr := flag.Int("runs", 1, "number of runs per client")
   setPtr := flag.Float64("setp", 0.1, "probability of set")
   flag.Parse()

   /*if *cpuprofile != "" {
     f, err := os.Create(*cpuprofile)
     if err != nil {
        log.Fatal(err)
     }
     pprof.StartCPUProfile(f)
     defer pprof.StopCPUProfile()
   }*/
   
   numClients := *numPtr
   numRuns := *runPtr
   setProb := *setPtr

   //mc := memcache.New("localhost:11211")
   mc := memcache.New("10.1.212.210:2888")
   mc.MaxIdleConns = numClients+10//(numClients/2)
   wg := new(sync.WaitGroup)
   start := make(chan bool)

   // start clients
   for i := 0; i < numClients; i++ {
      wg.Add(1)
      go client(wg, start, mc, numRuns, setProb)
   }

   // wait for clients to setup
   time.Sleep(time.Second*3)

   fmt.Println("Start...")
   starttime := time.Now()
   for i := 0; i < numClients; i++ {
      start <- true
   }
   wg.Wait()
   duration := time.Since(starttime).Seconds()

   numReqs := numRuns*numClients
   fmt.Printf("Throughput[KReq/s]: %2f\n", float64(numReqs) / duration / 1000)
   fmt.Printf("Duration[ms]: %2f\n", duration*1000)

}
