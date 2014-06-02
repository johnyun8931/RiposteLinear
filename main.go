package main

import (
  "fmt"
  "io"
  "log"
  "net"
  "os/exec"
  "strconv"
)

import (
  "henrycg/email/utils"
)

func readAll(p io.ReadCloser) {
  for {
    var str [1024]byte
    p.Read(str[:])
    fmt.Printf("> %s", str)
  }
}

func main() {
  var servers = utils.AllServers()

  var procs []*exec.Cmd = make([]*exec.Cmd, len(servers))
  for i := range servers {
    _, port, err := net.SplitHostPort(servers[i])
    log.Printf("Starting server: %v", servers[i])
    if err != nil {
      log.Fatal("Oh no!")
      return
    }
    procs[i] = exec.Command("./server", strconv.Itoa(i), port)
    stdout, err := procs[i].StdoutPipe()
    stderr, err := procs[i].StderrPipe()
    go readAll(stdout)
    go readAll(stderr)
    err = procs[i].Start()
    if err != nil {
      log.Printf("Process %v error: %v", i, err.Error())
    }
  }

  for i := 0; i<len(servers); i++ {
    err := procs[i].Wait()
    if err != nil {
      log.Printf("Process %v error: %v", i, err.Error())
    }
  }
}

