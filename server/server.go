package main

import (
  "log"
  "net"
  "net/http"
  "net/rpc"
  "os"
)

import (
  "henrycg/email/db"
)

func main() {
  if len(os.Args) != 2 {
    log.Fatal("Usage: ", os.Args[0], " [port]")
    return
  }

  port := os.Args[1]
  slot_table := new(db.SlotTable)
  rpc.Register(slot_table)
  rpc.HandleHTTP()
  log.Printf("Listening at :", port)

  err := http.ListenAndServe(net.JoinHostPort("", port), nil)
  if err != nil {
    log.Fatal("Error:", err.Error())
  }
}

