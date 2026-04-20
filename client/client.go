package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"sync"

	//"bytes"
	//"encoding/gob"

	"bitbucket.org/henrycg/riposte/db"
	"bitbucket.org/henrycg/riposte/utils"
)

var donothingFlag = flag.Bool("donothing", false, "If set, client pings server.")
var bogusFlag = flag.Bool("bogus", false, "If set, client sends an invalid request.")
var hammerFlag = flag.Bool("hammer", false, "If set, client sends requests to server as quickly as possible.")
var coordinatorFlag = flag.String("coordinator", "", "Coordinator IP and port")
var leaderFlag = flag.String("leader", "", "Riposte pair leader IP and port")
var logFlag = flag.String("log", "", "Location of log file")
var threadsFlag = flag.Uint("threads", 1, "Number of threads to use")

var countLock sync.Mutex
var count int

func tryUpload(client *rpc.Client, msg *db.Plaintext) error {
	var upRes1 db.UploadReply1
	var upArgs1 db.UploadArgs1

	msgBitShares, err := db.InitializeUploadArgs(&upArgs1, msg, *bogusFlag)
	if err != nil {
		panic("Error initializing upload args")
	}

	//var buf []byte
	//b := bytes.NewBuffer(buf)
	//g := gob.NewEncoder(b)
	//g.Encode(upArgs1)
	//log.Printf("Buffer len %v", b.Len())

	err = client.Call("Server.Upload1", &upArgs1, &upRes1)
	if err != nil {
		log.Printf("Error: %v", err)
		return err
	}

	var upRes2 db.UploadReply2
	mint, upArgs2 := db.SetUploadArgs2(msgBitShares, &upArgs1, &upRes1)

	// Get second msg
	err = client.Call("Server.Upload2", &upArgs2, &upRes2)
	if err != nil {
		log.Printf("Error: %v", err)
		return err
	}

	var upRes3 db.UploadReply3
	upArgs3 := db.SetUploadArgs3(msg, mint, &upArgs1, &upRes1, upArgs2, &upRes2)

	// Get third msg
	err = client.Call("Server.Upload3", &upArgs3, &upRes3)
	if err != nil {
		log.Printf("Error: %v", err)
		return err
	}

	return nil
}

func runClient(server string, msg *db.Plaintext) {
	certs := make([]tls.Certificate, 1)
	certs[0] = utils.LeaderCertificate
	client, err := utils.DialHTTPWithTLS("tcp", server, -1, certs)
	if err != nil {
		log.Printf("Could not connect: %v", err)
		return
	}
	defer client.Close()

	//log.Printf("Connected")
	for {
		c := -1
		countLock.Lock()
		count += 1
		c = count
		countLock.Unlock()

		if c%100 == 0 {
			log.Printf("Sent %v requests", c)
		}

		if *donothingFlag {
			var a, b int
			err := client.Call("Server.DoNothing", &a, &b)
			if err != nil {
				panic("Oh no!")
			}

		} else {
			err = tryUpload(client, msg)
			if err != nil {
				log.Printf("Upload error: %v", err)
				return
			}
		}

		if !*hammerFlag {
			break
		}
	}
}

func clientOnce(target string) {
	if *donothingFlag {
		runClient(target, nil)
	} else {
		//log.Printf("=== Starting Client ===")
		msg, err := db.RandomMessage()

		if err != nil {
			log.Printf("Error generating message: %v", err)
			return
		}

		//log.Printf("Insert into [%v,%v]", xIdx, yIdx)
		//log.Printf("Plaintext [%v]", msg)
		runClient(target, msg)
	}
}

func resolveTargetAddress(coordinatorAddr, leaderAddr string) (string, error) {
	if coordinatorAddr != "" && leaderAddr != "" {
		return "", errors.New("must specify only one of -coordinator or -leader")
	}
	if coordinatorAddr != "" {
		return coordinatorAddr, nil
	}
	if leaderAddr != "" {
		return leaderAddr, nil
	}
	return "", errors.New("must specify one of -coordinator or -leader")
}

func targetAddress() (string, error) {
	return resolveTargetAddress(*coordinatorFlag, *leaderFlag)
}

func main() {
	flag.Parse()
	target, err := targetAddress()
	if err != nil {
		log.Fatal(err)
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

	// Make one request
	if !*hammerFlag {
		clientOnce(target)
	} else {
		// Make many requests concurrently
		concurrent := 16
		var wg sync.WaitGroup
		wg.Add(concurrent)
		for i := 0; i < concurrent; i++ {
			go func() {
				defer wg.Done()
				clientOnce(target)
			}()
		}

		wg.Wait()
	}
}
