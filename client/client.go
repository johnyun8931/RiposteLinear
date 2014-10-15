package main

import (
  "crypto/tls"
  "flag"
  "fmt"
  "os"
  "net/rpc"
  "log"

  "henrycg/email/db"
  "henrycg/email/utils"
)

var bogusFlag = flag.Bool("bogus", false, "If set, client sends an invalid request.")
var leaderFlag = flag.String("leader", "", "Leader IP and port")

func tryUpload(client *rpc.Client, args db.UploadArgs) error {
  var upRes db.UploadReply

  err := client.Call("Server.Upload", args, &upRes)
  if err != nil {
    log.Fatal("Error:", err)
    return err
  }

  log.Printf("Got message!", upRes)
  return nil
}

func tryDumpTable(client *rpc.Client) db.DumpReply {
  var tab db.DumpReply
  err := client.Call("Server.DumpPlaintext", 0, &tab)
  if err != nil {
    log.Fatal("Error:", err)
  }

  return tab
}

func runClient(server string, args db.UploadArgs, tab *db.DumpReply) {
  certs := make([]tls.Certificate, 1)
  certs[0] = utils.LeaderCertificate
  client, err := utils.DialHTTPWithTLS("tcp", server, -1, certs)
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

  log.Printf("Done")
}

func main() {
  flag.Parse()
  if *leaderFlag == "" {
    fmt.Printf("Must specify leader.\n")
    os.Exit(1)
  }

  xIdx, yIdx, msg, err := db.RandomMessage()

  if err != nil {
    log.Fatal("error: ", err)
    return
  }

  log.Printf("Insert into [%v,%v]", xIdx, yIdx)
  log.Printf("Plaintext [%v]", msg)

  var args db.UploadArgs
  err = db.InitializeUploadArgs(&args, xIdx, yIdx, msg, *bogusFlag)
  if err != nil {
    log.Fatal("error: ", err)
    return
  }

  var table db.DumpReply
  runClient(*leaderFlag, args, &table)
}

