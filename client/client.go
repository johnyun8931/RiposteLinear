package main

import (
  "crypto/rand"
  "crypto/sha256"
  "fmt"
  "math/big"
  "net/rpc"
  "log"

  "henrycg/email/db"
  "henrycg/email/utils"
  "henrycg/zkp/commit"
)

func initializeUploadArgs(args *db.UploadArgs, xIdx int, yIdx int,
    msg db.SlotContents) error {
  var randVecsX [db.TABLE_WIDTH]bool
  var randVecsY [db.TABLE_HEIGHT]db.SlotContents

  utils.RandomVector(randVecsX[:])
  randomVectorMsg(randVecsY[:])

  xStar := !randVecsX[xIdx]
  yStar := db.AddSlots(randVecsY[yIdx], msg)

  var commitX db.CommitRow
  var commitXp db.CommitRow
  var commitY db.CommitCol
  var commitYp db.CommitCol

  var secX [db.TABLE_WIDTH]*big.Int
  var secXp [db.TABLE_WIDTH]*big.Int
  var secY [db.TABLE_HEIGHT]*big.Int
  var secYp [db.TABLE_HEIGHT]*big.Int

  for i := 0; i<db.TABLE_WIDTH; i++ {
    commitX[i], secX[i] = commit.Commit(big.NewInt(boolToInt(randVecsX[i])))
  }

  for i := 0; i<db.TABLE_HEIGHT; i++ {
    commitY[i], secY[i] = commit.Commit(hashString(randVecsY[i].Message[:]))
  }

  copy(commitXp[:], commitX[:])
  copy(commitYp[:], commitY[:])
  copy(secXp[:], secX[:])
  copy(secYp[:], secY[:])

  commitXp[xIdx], secXp[xIdx] = commit.Commit(big.NewInt(boolToInt(xStar)))
  commitYp[yIdx], secYp[yIdx] = commit.Commit(hashString(yStar.Message[:]))

  for i := 0; i < db.NUM_SERVERS; i++ {
    var plainQuery db.InsertQuery

    copy(plainQuery.XCoords[:], randVecsX[:])
    copy(plainQuery.YCoords[:], randVecsY[:])
    copy(plainQuery.XCommits[:], commitX[:])
    copy(plainQuery.XpCommits[:], commitXp[:])
    copy(plainQuery.YCommits[:], commitY[:])
    copy(plainQuery.YpCommits[:], commitYp[:])

    if (i & 1) == 0 {
      plainQuery.XCoords[xIdx] = xStar
    }

    if (i & 2) == 0 {
      plainQuery.YCoords[yIdx] = yStar
    }

    var err error
    args.Query[i], err = db.EncryptQuery(i, plainQuery)
    if err != nil {
      log.Fatal("Could not encrypt: ", err)
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

func boolToInt(b bool) int64 {
  if (b) {
    return 1
  } else {
    return 0
  }
}

func hashString(b []byte) *big.Int {
  h := sha256.Sum224(b)
  return new(big.Int).SetBytes(h[:])
}
