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

    if (i & 1) == 0 {
      args.Query[i].XCoords[msg_bit[0]] = !args.Query[i].XCoords[msg_bit[0]]
    }

    if (i & 2) == 0 {
      args.Query[i].YCoords[msg_bit[1]] = !args.Query[i].YCoords[msg_bit[1]]
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
  err := client.Call("SlotTable.DumpPlaintext", 0, &tab)
  if err != nil {
    log.Fatal("Error:", err)
  }

  return tab
}

func runClient(server string, args db.UploadArgs, tab *db.DumpReply) {

  client, err := utils.DialHTTPWithTLS("tcp", server, -1, utils.LeaderCertificate)
  if err != nil {
    log.Fatal("Could not connect:", err)
    return
  }

  err = tryUpload(client, args)
  if err != nil {
    log.Fatal("Upload error", err)
    return
  }
  log.Printf("Done uploading")
  *tab = tryDumpTable(client)

  log.Printf("Done")
}

func main() {
  var err error
  var msg_bit [db.NUM_DIMENSIONS]int

  msg_bit[0], err = utils.RandomInt(db.NUM_SLOTS)
  msg_bit[1], err = utils.RandomInt(db.NUM_SLOTS)

  var args db.UploadArgs
  err = initializeUploadArgs(&args, msg_bit[:])
  if err != nil {
    log.Fatal("error: ", err)
    return
  }

  var table db.DumpReply
  servers := utils.AllServers()
  leader := servers[0]
  runClient(leader, args, &table)

  for i := 0; i<db.NUM_SLOTS; i++ {
    for j := 0; j<db.NUM_SLOTS; j++ {
      var b int
      if (table.Entries[i][j].Bit) {
        b = 1
      } else {
        b = 0
      }
      fmt.Printf("%d", b)
    }
    fmt.Printf("\n")
    fmt.Printf("\n")
  }
}

