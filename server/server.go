package main

import (
  "log"
  "net"
  "net/http"
  "net/rpc"
  "os"
  "strconv"
)

import (
  "henrycg/email/db"
)

func main() {
  if len(os.Args) != 3 {
    log.Fatal("Usage: ", os.Args[0], " [index] [port]")
    return
  }

  idx,err := strconv.Atoi(os.Args[1])
  if err != nil {
    log.Fatal("Invalid index: %s", os.Args[1])
    return
  }

  port := os.Args[2]
  slot_table := new(db.SlotTable)
  slot_table.ServerIdx = idx
  slot_table.State = db.State_AcceptUpload
  var a int
  go slot_table.Initialize(&a, &a)

  rpc.Register(slot_table)
  rpc.HandleHTTP()
  log.Printf("Server %d is listening at %s", idx, port)

  http.ListenAndServe(net.JoinHostPort("", port), nil)
}

