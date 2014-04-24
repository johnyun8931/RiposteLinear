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
    procs[i].Start()
  }

  for i := 0; i<len(servers); i++ {
    procs[i].Wait()
  }
}

