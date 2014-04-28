package main

import (
  "log"
  "net"
  "os/exec"
)

import (
  "henrycg/email/utils"
)

func main() {
  var servers = utils.AllServers()

  var procs []*exec.Cmd = make([]*exec.Cmd, len(servers))
  for i := range servers {
    _, port, err := net.SplitHostPort(servers[i])
    log.Printf("Starting server: ", servers[i])
    if err != nil {
      log.Fatal("Oh no!")
      return
    }
    procs[i] = exec.Command("server/server", port)
    err = procs[i].Start()
    if err != nil {
      log.Printf("Process ", i, " error: ", err.Error())
    }
  }

  for i := 0; i<len(servers); i++ {
    err := procs[i].Wait()
    if err != nil {
      log.Printf("Process ", i, " error: ", err.Error())
    }
  }
}

