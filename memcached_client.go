package main

import (
   "github.com/dsidler/fpgamemcache/memcache"
   "fmt"
   "net"
   "bufio"
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
   runtime     int
   scans       int
   setProb     float64
   valueLength int
   zipfs       float64
   regex       bool
   matchProb   float64
}

type statistics struct {
   reqs int
   sets int
   gets int
   miss int
   setErrors int
   getErrors int
}
const letters = "0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz"

func GenerateKeys(numKeys int) []string {
   keys := make([]string, numKeys)
   // Generate keys in range
   pre := 0
   for i := 0; i < numKeys; i++ {
      keys[i] = "foobarba" + string(letters[i % len(letters)]) + string(letters[pre % len(letters)]) +"xxxxzz"
      if i+1 % len(letters) == 0 {
         pre++;
      }
   }

   // Check key length
   for _,key := range(keys) {
      if len(key) != 16 {
         fmt.Println("Error: key has unexpected length: ", len(key))
         os.Exit(1)
      }
   }
   return keys
}

//TODO Generate Skewed Keys

func client(wg * sync.WaitGroup, s chan bool, mc *memcache.Client, config *configuration, statschan chan statistics) {
   defer wg.Done()
   numKeys := 1000
   /*if config.zipfs != 0 {
      fmt.Println("Using zipf")
   }*/
   oracle := rand.New(rand.NewSource(time.Now().UnixNano()))
   r := rand.New(rand.NewSource(time.Now().UnixNano()))
   zipf := rand.NewZipf(r, config.zipfs, 1.0, uint64(numKeys))

   if config.regex {
      config.valueLength -= 2
   }

   // Generate keys
   keys := GenerateKeys(numKeys)
   // Generate value
   value := make([]byte, config.valueLength)
   for i := 0; i < config.valueLength; i++ {
      value[i] = letters[i % len(letters)]
   }
   matchingValue :=  append([]byte("0123456789abcdef"), value[32:]...)
   matchingValue = append(matchingValue, []byte("systemsgroupethz")...)


   stats := statistics{reqs: 0, sets: 0, gets: 0, setErrors: 0, getErrors: 0}

   // Open a connection by issuing a set/get
   var err error
   if config.regex {
      err = mc.SetJSON(&memcache.Item{Key: keys[0], Value: value})
   } else {
      err = mc.Set(&memcache.Item{Key: keys[0], Value: value})
   }
   if err != nil {
      fmt.Println("Error on open/set connection: ", err.Error())
      //os.Exit(1)
   }

   // Wait for start signal
   startsig := <-s
   if !startsig {
      fmt.Println("Wrong start signal.")
   }

   //map for zipf, for debug
   //distr := make(map[string]int)

   keyidx := uint64(0)
   for {
      select {
      case stopsig := <-s:
         //fmt.Println("go signal")
         if !stopsig {
            statschan <- stats
            return
         }
      default:
      stats.reqs++
      if config.zipfs != 0 {
         keyidx = zipf.Uint64()
      } else {
         keyidx = uint64(stats.reqs % numKeys)
      }
      // For debug only
      /*if _, ok := distr[keys[keyidx]]; !ok{
         distr[keys[keyidx]] = 0
      }
      distr[keys[keyidx]]++*/

      prob := oracle.Float64()
      var err error
      //if runs % 10 == 0 {
      if prob < config.setProb {
         if config.regex {
            //matching prob
            prob := oracle.Float64()
            if prob < config.matchProb {
               err = mc.SetJSON(&memcache.Item{Key: keys[keyidx], Value: matchingValue})
            } else {
               err = mc.SetJSON(&memcache.Item{Key: keys[keyidx], Value: value})
            }
         } else {
            err = mc.Set(&memcache.Item{Key: keys[keyidx], Value: value})
         }
         if err != nil {
            fmt.Println("Error on set: ", err.Error())
            stats.setErrors++
            //os.Exit(1)
         } else {
            stats.sets++
         }
      } else {
         //TODO regex config
         regex := []byte("0123456789abcdefsystemsgroupethz")
         if config.regex {
            _, err = mc.Ret(&memcache.Item{Key: keys[keyidx], Value: regex}, config.scans)
         } else {
            _, err = mc.Get(keys[keyidx], config.scans)
         }
         if err != nil {
            fmt.Println("Error on get/ret: ", err.Error())
            stats.getErrors++
            //os.Exit(1)
         } else {
            stats.gets++
         }
      }

   } //select
   } //for
   //print out distribution, for debug
   /*for s,c := range distr {
      fmt.Println(s, "\t", c)
   }*/
}

func scan_client(wg * sync.WaitGroup, s chan bool, mc *memcache.Client, config *configuration, statschan chan statistics) {
   defer wg.Done()
   numKeys := 1000

   if config.regex {
      config.valueLength -= 2
   }

   // Generate keys
   keys := GenerateKeys(numKeys)
   // Generate value
   value := make([]byte, config.valueLength)
   for i := 0; i < config.valueLength; i++ {
      value[i] = letters[i % len(letters)]
   }
   matchingValue :=  append([]byte("0123456789abcdef"), value[32:]...)
   matchingValue = append(matchingValue, []byte("systemsgroupethz")...)

   stats := statistics{reqs: 0, sets: 0, gets: 0, miss: 0, setErrors: 0, getErrors: 0}

   // Open a connection by issuing a set/get
   var err error
   if config.regex {
      err = mc.SetJSON(&memcache.Item{Key: keys[0], Value: value})
   } else {
      err = mc.Set(&memcache.Item{Key: keys[0], Value: value})
   }
   if err != nil {
      fmt.Println("Error on open/set connection: ", err.Error())
      //os.Exit(1)
   }

   // Wait for start signal
   startsig := <-s
   if !startsig {
      fmt.Println("Wrong start signal.")
   }

   keyidx := uint64(0)
   doSets := true
   numKeys -= config.scans
   for {
      select {
      case stopsig := <-s:
         //fmt.Println("go signal")
         if !stopsig {
            statschan <- stats
            return
         }
      default:
      var err error
      if doSets {
         for i := 0; i < config.scans; i++ {
            stats.reqs++
            if i % 2 == 0 {
               err = mc.SetJSON(&memcache.Item{Key: keys[keyidx+uint64(i)], Value: value})
            } else {
               err = mc.SetJSON(&memcache.Item{Key: keys[keyidx+uint64(i)], Value: matchingValue})
            }
            if err != nil {
               fmt.Println("Error on set: ", err.Error())
               stats.setErrors++
               //os.Exit(1)
            } else {
               stats.sets++
            }
         } //for
         doSets = false
      } else {
         stats.reqs++
         //regex := []byte("0123456789abcdef0123456789abcdef")
         regex := []byte("0123456789abcdefsystemsgroupethz")
         it, err := mc.Ret(&memcache.Item{Key: keys[keyidx], Value: regex}, config.scans)
         if err != nil {
            fmt.Println("Error on get/ret: ", err.Error())
            stats.getErrors++
            //os.Exit(1)
         } else {
            stats.gets++
         }
         if it == nil {
            stats.miss++
         }
         //update key
         keyidx = uint64(stats.reqs % numKeys)
         doSets = true
      }

   } //select
   } //for
   //print out distribution, for debug
   /*for s,c := range distr {
      fmt.Println(s, "\t", c)
   }*/
}

func udp_client(wg * sync.WaitGroup, s chan bool, mc *memcache.Client, config *configuration, statschan chan statistics) {
   defer wg.Done()
   numKeys := 1000
   oracle := rand.New(rand.NewSource(time.Now().UnixNano()))
   r := rand.New(rand.NewSource(time.Now().UnixNano()))
   zipf := rand.NewZipf(r, config.zipfs, 1.0, uint64(numKeys))

   //Create connection
   conn, err := net.Dial("udp", config.host)
   if err != nil {
      fmt.Println("Error on dial: ", err.Error())
      os.Exit(1)
   }
   rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

   // Generate keys
   keys := GenerateKeys(numKeys)
   // Generate value
   value := make([]byte, config.valueLength)
   for i := 0; i < config.valueLength; i++ {
      value[i] = letters[i % len(letters)]
   }

   stats := statistics{reqs: 0, sets: 0, gets: 0, setErrors: 0, getErrors: 0}

   // Wait for start signal
   startsig := <-s
   if !startsig {
      fmt.Println("Wrong start signal.")
   }

      keyidx := uint64(0)
   //for ; runs < config.numRuns; runs++ {
   for {
      select {
   case stopsig := <-s:
      fmt.Println("go signal")
      if !stopsig {
         statschan <- stats
         return
      }
   default:
      stats.reqs++ //TODO what about fails??
      if config.zipfs != 0 {
         keyidx = zipf.Uint64()
      } else {
         keyidx = uint64(stats.reqs % numKeys)
      }

      prob := oracle.Float64()

      conn.SetReadDeadline(time.Now().Add(1000 * time.Millisecond))
      if prob < config.setProb {
         err := mc.SetUDP(rw, &memcache.Item{Key: keys[keyidx], Value: value})
         if err != nil {
            fmt.Println("Error on set: ", err.Error())
            stats.setErrors++
            //os.Exit(1)
         } else {
            stats.sets++
         }
      } else {
         //TODO regex config
         //regex := []byte("0123456789abcdef0123456789abcdef")
         _, err := mc.GetUDP(rw, keys[keyidx], config.scans)
         //_, err := mc.RetUDP(rw, &memcache.Item{Key: keys[keyidx], Value: regex}, config.scans)
         if err != nil {
            fmt.Println("Error on get: ", err.Error())
            stats.getErrors++
            //os.Exit(1)
         } else {
            stats.gets++
         }
      }
      } // select
   } //for
   fmt.Println("done")
}

//var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")


func main() {
   //runtime.GOMAXPROCS(8)
   hostPtr := flag.String("host", "10.1.212.209:2888", "host addr and port")
   udpPtr := flag.Bool("udp", false, "use UDP")
   numPtr := flag.Int("clients", 1, "number of clients")
   timePtr := flag.Int("time", 10, "runtime in seconds")
   scanPtr := flag.Int("scans", 1, "number of scans")
   setPtr := flag.Float64("setp", 0.1, "probability of set")
   valuePtr := flag.Int("vallen", 64, "length of value")
   zipfPtr := flag.Float64("zipfs", 0.0, "zipf value s")
   zsoltPtr := flag.Bool("zsolt", true, "use zsolts protocol")
   regexPtr := flag.Bool("regex", false, "use regex mode")
   matchPtr := flag.Float64("regexmatch", 0.5, "probability of regex match")
   flag.Parse()

   /*if *cpuprofile != "" {
     f, err := os.Create(*cpuprofile)
     if err != nil {
        log.Fatal(err)
     }
     pprof.StartCPUProfile(f)
     defer pprof.StopCPUProfile()
   }*/

   useUDP := *udpPtr
   config := configuration {
               host: *hostPtr,
               numClients: *numPtr,
               runtime: *timePtr,
               scans: *scanPtr,
               setProb: *setPtr,
               valueLength: *valuePtr,
               zipfs: *zipfPtr,
               regex: *regexPtr,
               matchProb: *matchPtr}
   if config.valueLength % 64  != 0 {
      fmt.Println("value length Must be multiple of 64")
      os.Exit(1)
   }
   // Adapt value length
   config.valueLength -= 8
   if config.zipfs != 0 && config.zipfs <= 1 {
      fmt.Println("zifps must be >1 or 0.0")
      os.Exit(1)
   }

   //TODO make host as input
   mc := memcache.New(config.host)
   // Set max idle cons
   mc.MaxIdleConns = config.numClients+10//(numClients/2)
   // set network timeout
   mc.Timeout = 5000 * time.Millisecond
   mc.UseZsolt = *zsoltPtr

   wg := new(sync.WaitGroup)
   start := make(chan bool)
   statschan := make(chan statistics)

   // start clients
   for i := 0; i < config.numClients; i++ {
      wg.Add(1)
      if useUDP {
         go udp_client(wg, start, mc, &config, statschan)
      } else {
         if config.scans != 1 {
            go scan_client(wg, start, mc, &config, statschan)
         } else {
            go client(wg, start, mc, &config, statschan)
         }
      }
   }

   // wait for clients to setup
   time.Sleep(time.Second*3)

   fmt.Println("Start...")
   starttime := time.Now()
   for i := 0; i < config.numClients; i++ {
      start <- true
   }
   // Run for x seconds
   time.Sleep(time.Duration(config.runtime) * time.Second)

   // Stop clients
   fmt.Println("Stopping...")
   for i :=0; i < config.numClients; i++ {
      start <- false
   }
   stats := make([]statistics, config.numClients)
   gstats := statistics{reqs: 0, sets: 0, gets: 0, miss: 0, setErrors: 0, getErrors: 0}
   for i :=0; i < config.numClients; i++ {
      stats[i] = <- statschan
      gstats.reqs += stats[i].reqs
      gstats.sets += stats[i].sets
      gstats.gets += stats[i].gets
      gstats.setErrors += stats[i].setErrors
      gstats.getErrors += stats[i].getErrors
   }
   wg.Wait()
   duration := time.Since(starttime).Seconds()

   fmt.Println("----------------------------")
   fmt.Printf("Throughput[KReq/s]: %2f\n", float64(gstats.reqs) / duration / 1000)
   fmt.Printf("Duration[s]: %2f\n", duration)
   fmt.Println("Requests: ", gstats.reqs)
   fmt.Println("Sets: ", gstats.sets)
   fmt.Println("Gets: ", gstats.gets)
   fmt.Println("Misses: ", gstats.miss)
   fmt.Println("Set errors: ", gstats.setErrors)
   fmt.Println("Get errors: ", gstats.getErrors)

}
