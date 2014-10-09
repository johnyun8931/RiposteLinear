package main

import (
  "crypto/tls"
  "fmt"
  "log"
  "net"
  "net/rpc"
  "os"
  "os/signal"
  "runtime/pprof"
  "strconv"
)

import (
  "henrycg/email/db"
  "henrycg/email/utils"
)

func main() {
  if len(os.Args) < 3 || len(os.Args) > 4 {
    log.Fatal("Usage: ", os.Args[0], " [index] [port] [--profile]")
    return
  }

  idx,err := strconv.Atoi(os.Args[1])
  if err != nil {
    log.Fatal("Invalid index: %s", os.Args[1])
    return
  }

  if len(os.Args) > 3 {
    if os.Args[3] == "--profile" {
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
    } else {
      log.Fatal("Invalid profile flag")
    }
  }

  port := os.Args[2]
  slotTable := db.NewServer(idx)
  var a int
  go slotTable.Initialize(&a, &a)

  log.SetPrefix(fmt.Sprintf("[Server %v] ", idx))

  rpc.Register(slotTable)
  //rpc.HandleHTTP()
  addr := net.JoinHostPort("", port)

  var certs []tls.Certificate

  // If we are not the leader, only allow 
  // connections from the leader
  if idx > 0 {
    certs = append(certs, utils.LeaderCertificate)
  }
  utils.ListenAndServe(addr, idx, certs)
  log.Printf("Server %d is listening at %s", idx, port)

  //http.ListenAndServe(addr, nil)
}
