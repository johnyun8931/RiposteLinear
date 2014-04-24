package main

import (
  "net/rpc"
  "log"

  "henrycg/email/db"
  "henrycg/email/utils"
)


func tryUpload(client *rpc.Client) {
  var upArgs db.UploadArgs

  // XXX arbitrary constant
  var err error;
  upArgs.DestinationSlot, err = utils.RandomInt(db.NUM_SLOTS)
  if err != nil {
    log.Fatal("Error:", err)
  }
  upArgs.Message.Buffer[0] = 'A'
  var upRes db.UploadReply

  err = client.Call("SlotTable.Upload", upArgs, &upRes)
  if err != nil {
    log.Fatal("Error:", err)
  }

  log.Printf("Got message!", upRes)
}

func tryDownload(client *rpc.Client) {
  var downArgs db.DownloadArgs

  idx, err := utils.RandomInt(db.NUM_SLOTS)
  if err != nil {
    log.Fatal("Error:", err)
  }
  downArgs.RequestedSlot = idx
  log.Printf("Request slot", idx)

  var downRes db.DownloadReply
  err = client.Call("SlotTable.Download", downArgs, &downRes)
  if err != nil {
    log.Fatal("Error:", err)
  }

  log.Printf("Got message!", downRes)

}

func runClient(server string, c chan int) {
  client, err := rpc.DialHTTP("tcp", server)
  if err != nil {
    log.Fatal("Could not connect:", err)
  }

  tryUpload(client)
  tryDownload(client)

  c <- 1
}

func main() {
  c := make(chan int, utils.NumServers())
  servers := utils.AllServers()
  for i := range servers {
    go runClient(servers[i], c)
  }

  for i := 0; i<len(servers); i++ {
    <-c
  }
}
