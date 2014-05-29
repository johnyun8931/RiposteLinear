package main

import (
  "crypto/rand"
  "fmt"
  "net/rpc"
  "log"

  "henrycg/email/db"
  "henrycg/email/utils"
)

func initializeUploadArgs(args *db.UploadArgs, xIdx int, yIdx int,
    msg db.SlotContents) error {
  var randVecsX [db.TABLE_WIDTH]bool
  var randVecsY [db.TABLE_HEIGHT]db.SlotContents

  utils.RandomVector(randVecsX[:])
  randomVectorMsg(randVecsY[:])

  for i := 0; i < db.NUM_SERVERS; i++ {
    var plainQuery db.InsertQuery

    copy(plainQuery.XCoords[:], randVecsX[:])
    copy(plainQuery.YCoords[:], randVecsY[:])

    if (i & 1) == 0 {
      plainQuery.XCoords[xIdx] = !plainQuery.XCoords[xIdx]
    }

    if (i & 2) == 0 {
      old := plainQuery.YCoords[yIdx]
      plainQuery.YCoords[yIdx] = db.AddSlots(old, msg)
    }

    var err error
    args.Query[i], err = db.EncryptQuery(i, plainQuery)
    if err != nil {
      log.Fatal("Could not encrypt")
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
  var xIdx, yIdx int
  var msg db.SlotContents

  xIdx, err = utils.RandomInt(db.TABLE_WIDTH)
  yIdx, err = utils.RandomInt(db.TABLE_HEIGHT)

  _, err = rand.Read(msg.Message[:])
  if err != nil {
    log.Fatal("error: ", err)
    return
  }

  log.Printf("Insert into [%v,%v]", xIdx, yIdx)
  log.Printf("Plaintext [%v]", msg.Message)


  var args db.UploadArgs
  err = initializeUploadArgs(&args, xIdx, yIdx, msg)
  if err != nil {
    log.Fatal("error: ", err)
    return
  }

  var table db.DumpReply
  servers := utils.AllServers()
  leader := servers[0]
  runClient(leader, args, &table)

  for i := 0; i<db.TABLE_WIDTH; i++ {
    for j := 0; j<db.TABLE_HEIGHT; j++ {
      fmt.Printf("%v", table.Entries[i][j].Message)
    }
    fmt.Printf("\n")
  }
}

func randomVectorMsg(lst []db.SlotContents) error {
  var err error
  for i := 0; i < len(lst); i++ {
    _, err = rand.Read(lst[i].Message[:])
    if err != nil {
      return err
    }
  }

  return nil
}

