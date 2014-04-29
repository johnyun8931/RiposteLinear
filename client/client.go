package main

import (
  "fmt"
  "net/rpc"
  "log"

  "henrycg/email/db"
  "henrycg/email/utils"
)

func randomVector(lst []bool) error {
  for i := 0; i<db.NUM_SLOTS; i++ {
    bit, err := utils.RandomInt(1)
    if err != nil {
      log.Fatal("error: ", err)
      return err
    }
    lst[i] = (bit != 0)
  }

  return nil
}

func initializeUploadArgs(args []db.UploadArgs, msg_bit []int) error {
  var randVecs [db.NUM_DIMENSIONS][db.NUM_SLOTS]bool

  for dim := 0; dim < db.NUM_DIMENSIONS; dim++ {
    err := randomVector(randVecs[dim][:])
    if err != nil {
      log.Fatal("Error: ", err)
      return err
    }
  }

  for i := 0; i < db.NUM_SERVERS; i++ {
    randomVector(args[i].XCoords[:])
    randomVector(args[i].YCoords[:])
    randomVector(args[i].ZCoords[:])
    /*
    copy(args[i].XCoords[:], randVecs[0][:])
    copy(args[i].YCoords[:], randVecs[1][:])
    copy(args[i].ZCoords[:], randVecs[2][:])

    if (i & 1) == 0 {
      bit := args[i].XCoords[msg_bit[0]]
      args[i].XCoords[msg_bit[0]] = !bit
    }

    if (i & 2) == 0 {
      bit := args[i].YCoords[msg_bit[1]]
      args[i].YCoords[msg_bit[1]] = !bit
    }

    if (i & 4) == 0 {
      bit := args[i].ZCoords[msg_bit[2]]
      args[i].ZCoords[msg_bit[2]] = !bit
    }
    */
  }

  return nil
}

func tryUpload(client *rpc.Client, args db.UploadArgs) {
  var upRes db.UploadReply

  err := client.Call("SlotTable.Upload", args, &upRes)
  if err != nil {
    log.Fatal("Error:", err)
  }

  log.Printf("Got message!", upRes)
}

func tryDumpTable(client *rpc.Client) db.DumpReply {
  var tab db.DumpReply
  err := client.Call("SlotTable.DumpTable", 0, &tab)
  if err != nil {
    log.Fatal("Error:", err)
  }

  return tab
}

func runClient(server string, args db.UploadArgs, tab *db.DumpReply, c chan int) {
  client, err := rpc.DialHTTP("tcp", server)
  if err != nil {
    log.Fatal("Could not connect:", err)
  }

  tryUpload(client, args)
  log.Printf("Done uploading")
  *tab = tryDumpTable(client)

  log.Printf("Done")
  c <- 1
}

func main() {
  var msg_bit [db.NUM_DIMENSIONS]int

  msg_bit[0] = 1
  msg_bit[1] = 2
  msg_bit[2] = 3

  var args [db.NUM_SERVERS]db.UploadArgs
  err := initializeUploadArgs(args[:], msg_bit[:])
  if err != nil {
    log.Fatal("error: ", err)
    return
  }

  var tables [db.NUM_SERVERS]db.DumpReply
  c := make(chan int, utils.NumServers())
  servers := utils.AllServers()
  for i := range servers {
    go runClient(servers[i], args[i], &tables[i], c)
  }

  for i := 0; i<len(servers); i++ {
    <-c
  }

  //var plaintext [db.NUM_SLOTS][db.NUM_SLOTS][db.NUM_SLOTS]bool
  for i := 0; i<db.NUM_SLOTS; i++ {
    for j := 0; j<db.NUM_SLOTS; j++ {
      for k := 0; k<db.NUM_SLOTS; k++ {
        /*
        bit := false
        for serv := 0; serv<db.NUM_SERVERS; serv++ {
          if tables[serv].Entries[i][j][k].Bit {
            bit = !bit
          }
        }
        */
        var b int
        if (tables[0].Entries[i][j][k].Bit) {
          b = 1
        } else {
          b = 0
        }
        fmt.Printf("%d", b)
      }
      fmt.Printf("\n")
    }
      fmt.Printf("\n")
      fmt.Printf("\n")
  }
}

