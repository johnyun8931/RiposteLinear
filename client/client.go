package main

import (
  //"crypto/rand"
  "crypto/tls"
//  "fmt"
  "net/rpc"
  "log"

  "bitbucket.org/henrycg/riposte/db"
  "bitbucket.org/henrycg/riposte/utils"
)

func tryUpload(client *rpc.Client, args db.UploadArgs) error {
  var upRes db.UploadReply

  err := client.Call("Server.Upload", args, &upRes)
  if err != nil {
    log.Fatal("Error:", err)
    return err
  }

  log.Println("Got message!", upRes)
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
  //*tab = tryDumpTable(client)

  log.Printf("Done")
}

func main() {
  xIdx, yIdx, msg, err := db.RandomMessage()

  if err != nil {
    log.Fatal("error: ", err)
    return
  }

  log.Printf("Insert into [%v,%v]", xIdx, yIdx)
  log.Printf("Plaintext [%v]", msg)

  var args db.UploadArgs
  err = db.InitializeUploadArgs(&args, xIdx, yIdx, msg)
  if err != nil {
    log.Fatal("error: ", err)
    return
  }

  var table db.DumpReply
  servers := utils.AllServers()
  leader := servers[0]
  runClient(leader, args, &table)

  /*
  for i := 0; i<db.TABLE_WIDTH; i++ {
    for j := 0; j<db.TABLE_HEIGHT; j++ {
      fmt.Printf("%v", table.Entries[i][j].Message)
    }
    fmt.Printf("\n")
  }
  */
}

