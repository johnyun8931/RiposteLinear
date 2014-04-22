package main

import "log"
import "net/http"
import "net/rpc"

import "./mailserver"

func main() {
  slot_table := new(mailserver.SlotTable)
  rpc.Register(slot_table)
  rpc.HandleHTTP()
  err := http.ListenAndServe(":8080", nil)
  if err != nil {
    log.Fatal("Error:", err.Error())
  }
}

/*
func getHandler(w http.ResponseWriter, r *http.Request, slot_table mailserver.SlotTable) {
  var idx = rand.Intn(mailserver.NUM_SLOTS)
  var slot *mailserver.SlotData = &slot_table.Entries[idx]
  slot.Mutex.Lock()
  slot.Is_filled = true
  slot.Mutex.Unlock()
  log.Printf("Connection from %v closed.", r.RemoteAddr)
}
*/

