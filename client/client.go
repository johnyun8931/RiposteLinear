package main

import (
  "crypto/tls"
  "flag"
  "log"
  "net/rpc"
  "os"
  "runtime"

  "henrycg/email/db"
  "henrycg/email/utils"
)

var bogusFlag = flag.Bool("bogus", false, "If set, client sends an invalid request.")
var hammerFlag = flag.Bool("hammer", false, "If set, client sends requests to server as quickly as possible.")
var leaderFlag = flag.String("leader", "", "Leader IP and port")
var logFlag = flag.String("log", "", "Location of log file")
var threadsFlag = flag.Uint("threads", 1, "Number of threads to use")

func tryUpload(client *rpc.Client, args db.UploadArgs) error {
  var upRes db.UploadReply

  err := client.Call("Server.Upload", args, &upRes)
  if err != nil {
    log.Printf("Error:", err)
    return err
  }

  log.Printf("Got message!", upRes)
  return nil
}

func tryDumpTable(client *rpc.Client) db.DumpReply {
  var tab db.DumpReply
  err := client.Call("Server.DumpPlaintext", 0, &tab)
  if err != nil {
    log.Printf("Error:", err)
  }

  return tab
}

func runClient(server string, args db.UploadArgs, tab *db.DumpReply) {
  certs := make([]tls.Certificate, 1)
  certs[0] = utils.LeaderCertificate
  client, err := utils.DialHTTPWithTLS("tcp", server, -1, certs)
  if err != nil {
    log.Printf("Could not connect:", err)
    return
  }

  err = tryUpload(client, args)
  if err != nil {
    log.Printf("Upload error", err)
    return
  }
  log.Printf("Done uploading")
  client.Close()
}

func clientOnce(bogus bool) {
  log.Printf("=== Starting Client ===")
  xIdx, yIdx, msg, err := db.RandomMessage()

  if err != nil {
    log.Printf("Error generating message: ", err)
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

func clientHammer(bogus bool) {
  for {
    clientOnce(*bogusFlag)
  }
}

func main() {
  flag.Parse()
  if *leaderFlag == "" {
    log.Fatal("Must specify leader.\n")
  }

  if *logFlag != "" {
    f, ferr := os.OpenFile(*logFlag, os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0664)
    if ferr != nil {
      log.Fatal("Could not open log file ", *logFlag)
    }
    log.SetOutput(f)
  }

  runtime.GOMAXPROCS(int(*threadsFlag))

  defer log.Printf("Client died.")

  // Make one request
  if !*hammerFlag {
    clientOnce(*bogusFlag)
  } else {
    // Make many requests concurrently
    concurrent := 8
    for i := 0; i < concurrent; i++ {
      clientHammer(*bogusFlag)
    }
  }
}

