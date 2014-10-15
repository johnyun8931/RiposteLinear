package main

import (
  "crypto/tls"
  "errors"
  "flag"
  "fmt"
  "log"
  "net/rpc"
  "os"
  "os/signal"
  "runtime/pprof"
  "strings"
)

import (
  "henrycg/email/db"
  "henrycg/email/utils"
)

var flagProfile = flag.Bool("profile", false, "Run CPU profiler")
var flagIndex = flag.Int("idx", -1, "Server index")

// List of server addresses
type serverListType []string
var serverList serverListType

func (s *serverListType) String() string {
  return fmt.Sprint(*s)
}

// Comma-separated list of server addresses (ip:port)
func (s *serverListType) Set(value string) error {
  if len(*s) > 0 {
    return errors.New("server flag already set")
  }

  for _, dt := range strings.Split(value, ",") {
    *s = append(*s, dt)
  }
  return nil
}

func init() {
  flag.Var(&serverList, "servers", "Comma-separated list of servers (in \"ip:port\" form)")
}

func main() {
  flag.Parse()

  if *flagIndex < 0 {
    log.Fatal("Must server index must be greater than zero")
    return
  }


  idx := *flagIndex

  if len(serverList) < 1 || idx > len(serverList) - 1 {
    log.Fatal("Must specify a list of servers")
    return
  }

  log.SetPrefix(fmt.Sprintf("[Server %v] ", idx))

  if *flagProfile {
    f, err := os.Create(fmt.Sprintf("server-%v.prof", idx))
    if err != nil {
      log.Fatal(err)
    }
    pprof.StartCPUProfile(f)

    // Stop when process exits
    defer pprof.StopCPUProfile()

    // Stop on ^C
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt)
    go func(){
      for _ = range c {
        // sig is a ^C, handle it
        pprof.StopCPUProfile()
        os.Exit(0)
      }
    }()
  }

  if idx == db.AUDIT_SERVER {
    auditor := new(db.Auditor)
    rpc.Register(auditor)
  } else {
    var a int
    slotTable := db.NewServer(idx, serverList)
    slotTable.Initialize(&a, &a)
    rpc.Register(slotTable)
  }

  var certs []tls.Certificate

  // If we are not the leader, only allow 
  // connections from the leader
  if idx > 0 {
    certs = append(certs, utils.LeaderCertificate)
  }

  utils.ListenAndServe(serverList[idx], idx, certs)
  log.Printf("Server %d is listening at %s", idx, serverList[idx])

  //http.ListenAndServe(addr, nil)
}

