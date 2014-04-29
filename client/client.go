package main

import (
  "fmt"
  "net/rpc"
  "log"

  "henrycg/email/db"
  "henrycg/email/utils"
)

func initializeUploadArgs(args *db.UploadArgs, msg_bit []int) error {
  var randVecs [db.NUM_DIMENSIONS][db.NUM_SLOTS]bool

  for dim := 0; dim < db.NUM_DIMENSIONS; dim++ {
    err := utils.RandomVector(randVecs[dim][:])
    if err != nil {
      log.Fatal("Error: ", err)
      return err
    }
  }

  for i := 0; i < db.NUM_SERVERS; i++ {
    /*
    randomVector(args[i].XCoords[:])
    randomVector(args[i].YCoords[:])
    randomVector(args[i].ZCoords[:])
    */

    copy(args.Query[i].XCoords[:], randVecs[0][:])
    copy(args.Query[i].YCoords[:], randVecs[1][:])
    copy(args.Query[i].ZCoords[:], randVecs[2][:])

    if (i & 1) == 0 {
      args.Query[i].XCoords[msg_bit[0]] = !args.Query[i].XCoords[msg_bit[0]]
    }

    if (i & 2) == 0 {
      args.Query[i].YCoords[msg_bit[1]] = !args.Query[i].YCoords[msg_bit[1]]
    }

    if (i & 4) == 0 {
      args.Query[i].ZCoords[msg_bit[2]] = !args.Query[i].ZCoords[msg_bit[2]]
    }
  }

  return nil
}

func tryUpload(client *rpc.Client, args db.UploadArgs) error {
  var upRes db.UploadReply

  err := client.Call("SlotTable.Upload", args, &upRes)
  if err != nil {
    log.Fatal("Error:", err)
    return err
  }

  log.Printf("Got message!", upRes)
  return nil
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

  err = tryUpload(client, args)
  if err != nil {
    log.Fatal("Upload error", err)
  }
  log.Printf("Done uploading")
  *tab = tryDumpTable(client)

  log.Printf("Done")
  c <- 1
}

func main() {
  var err error
  var msg_bit [db.NUM_DIMENSIONS]int

  msg_bit[0], err = utils.RandomInt(db.NUM_SLOTS)
  msg_bit[1], err = utils.RandomInt(db.NUM_SLOTS)
  msg_bit[2], err = utils.RandomInt(db.NUM_SLOTS)

  var args db.UploadArgs
  err = initializeUploadArgs(&args, msg_bit[:])
  if err != nil {
    log.Fatal("error: ", err)
    return
  }

  var tables [db.NUM_SERVERS]db.DumpReply
  c := make(chan int, utils.NumServers())
  servers := utils.AllServers()
  for i := range servers {
    go runClient(servers[0], args, &tables[i], c)
  }

  for i := 0; i<len(servers); i++ {
    <-c
  }

  //var plaintext [db.NUM_SLOTS][db.NUM_SLOTS][db.NUM_SLOTS]bool
  for i := 0; i<db.NUM_SLOTS; i++ {
    for j := 0; j<db.NUM_SLOTS; j++ {
      for k := 0; k<db.NUM_SLOTS; k++ {

        bit := false
        //bit = tables[0].Entries[i][j][k].Bit
        for serv := 0; serv<db.NUM_SERVERS; serv++ {
          if tables[serv].Entries[i][j][k].Bit {
            bit = !bit
          }
        }

        var b int
        if (bit) {
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

