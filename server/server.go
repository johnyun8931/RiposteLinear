package main

import (
  "log"
  "net"
  "net/rpc"
  "os"
  "strconv"
)

import (
  "henrycg/email/db"
  "henrycg/email/utils"
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
  //rpc.HandleHTTP()
  addr := net.JoinHostPort("", port)
  utils.ListenAndServe(addr, idx, idx == 0)
  log.Printf("Server %d is listening at %s", idx, port)

  //http.ListenAndServe(addr, nil)
}

