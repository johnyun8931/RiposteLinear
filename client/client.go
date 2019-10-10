package main

import (
	"crypto/tls"
	"flag"
	"log"
	"net/rpc"
	"os"
	"runtime"

	"bytes"
	"encoding/gob"

	"bitbucket.org/henrycg/riposte/db"
	"bitbucket.org/henrycg/riposte/utils"
)

var donothingFlag = flag.Bool("donothing", false, "If set, client pings server.")
var bogusFlag = flag.Bool("bogus", false, "If set, client sends an invalid request.")
var hammerFlag = flag.Bool("hammer", false, "If set, client sends requests to server as quickly as possible.")
var leaderFlag = flag.String("leader", "", "Leader IP and port")
var logFlag = flag.String("log", "", "Location of log file")
var threadsFlag = flag.Uint("threads", 1, "Number of threads to use")

func tryUpload(client *rpc.Client, args db.UploadArgs1) error {
	var upRes1 db.UploadReply1

	var buf []byte
	b := bytes.NewBuffer(buf)
	g := gob.NewEncoder(b)
	g.Encode(args)
	log.Printf("Buffer len %v", b.Len())

	err := client.Call("Server.Upload1", args, &upRes1)
	if err != nil {
		log.Printf("Error:", err)
		return err
	}

	var upArgs2 db.UploadArgs2
	var upRes2 db.UploadReply2

	copy(upArgs2.HashKey[:], upRes1.HashKey[:])
	upArgs2.Uuid = upRes1.Uuid

	// Get second msg
	err = client.Call("Server.Upload2", &upArgs2, &upRes2)
	if err != nil {
		log.Printf("Error:", err)
		return err
	}

	var upArgs3 db.UploadArgs3
	var upRes3 db.UploadReply3

	copy(upArgs3.HashKey[:], upRes1.HashKey[:])
	upArgs3.Uuid = upRes1.Uuid

	// Get second msg
	err = client.Call("Server.Upload3", &upArgs3, &upRes3)
	if err != nil {
		log.Printf("Error:", err)
		return err
	}

	log.Printf("Got message!", upRes1)
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

func runClient(server string, args db.UploadArgs1, tab *db.DumpReply) {
	certs := make([]tls.Certificate, 1)
	certs[0] = utils.LeaderCertificate
	client, err := utils.DialHTTPWithTLS("tcp", server, -1, certs)
	if err != nil {
		log.Printf("Could not connect:", err)
		return
	}

	log.Printf("Connected")

	if *donothingFlag {
		var a, b int
		err := client.Call("Server.DoNothing", &a, &b)
		if err != nil {
			panic("Oh no!")
		}

		log.Printf("Done")
	} else {
		err = tryUpload(client, args)
		if err != nil {
			log.Printf("Upload error", err)
			return
		}
		log.Printf("Done uploading")
	}
	client.Close()
}

func clientOnce(bogus bool) {
	var args db.UploadArgs1
	var table db.DumpReply

	if !*donothingFlag {
		log.Printf("=== Starting Client ===")
		xIdx, yIdx, msg, err := db.RandomMessage()

		if err != nil {
			log.Printf("Error generating message: ", err)
			return
		}

		log.Printf("Insert into [%v,%v]", xIdx, yIdx)
		log.Printf("Plaintext [%v]", msg)

		err = db.InitializeUploadArgs(&args, xIdx, yIdx, msg, *bogusFlag)
		if err != nil {
			log.Fatal("error: ", err)
			return
		}
	}

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
		f, ferr := os.OpenFile(*logFlag, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)
		if ferr != nil {
			log.Fatal("Could not open log file ", *logFlag)
		}
		log.SetOutput(f)
	}

	log.SetPrefix("[Client ] ")

	runtime.GOMAXPROCS(int(*threadsFlag))

	defer log.Printf("Client died.")

	c := make(chan int, 1)
	// Make one request
	if !*hammerFlag {
		clientOnce(*bogusFlag)
	} else {
		// Make many requests concurrently
		concurrent := 16
		for i := 0; i < concurrent; i++ {
			go clientHammer(*bogusFlag)
		}

		// Wait forever
		<-c
	}
}
