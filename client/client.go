package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//"bytes"
	//"encoding/gob"

	"bitbucket.org/henrycg/riposte/db"
	"bitbucket.org/henrycg/riposte/utils"
)

var donothingFlag = flag.Bool("donothing", false, "If set, client pings server.")
var bogusFlag = flag.Bool("bogus", false, "If set, client sends an invalid request.")
var hammerFlag = flag.Bool("hammer", false, "If set, client sends requests to server as quickly as possible.")
var concurrencyFlag = flag.Uint("concurrency", 16, "Number of concurrent hammer workers")
var retryOverloadFlag = flag.Bool("retry-overload", false, "If set, hammer clients retry ready-queue overloads with backoff.")
var overloadBackoffInitialFlag = flag.Uint("overload-backoff-initial-ms", 10, "Initial ready-queue overload retry backoff in milliseconds.")
var overloadBackoffMaxFlag = flag.Uint("overload-backoff-max-ms", 250, "Maximum ready-queue overload retry backoff in milliseconds.")
var coordinatorFlag = flag.String("coordinator", "", "Coordinator IP and port")
var leaderFlag = flag.String("leader", "", "Riposte pair leader IP and port")
var logFlag = flag.String("log", "", "Location of log file")
var threadsFlag = flag.Uint("threads", 1, "Number of threads to use")
var xFlag = flag.Int("x", -1, "Exact column to write for deterministic uploads")
var yFlag = flag.Int("y", -1, "Exact row to write for deterministic uploads")
var payloadFlag = flag.String("payload", "", "Exact payload to write for deterministic uploads")

var countLock sync.Mutex
var count int

var randomMessage = db.RandomMessage

type messageProvider func() (*db.Plaintext, error)

type overloadRetryConfig struct {
	enabled        bool
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

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

func isNoActiveEpochError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "No active epoch")
}

func isOverloadError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "server overloaded: ready queue full")
}

func uploadWithOverloadRetry(msg *db.Plaintext, config overloadRetryConfig, upload func(*db.Plaintext) error, sleep func(time.Duration)) error {
	backoff := config.initialBackoff
	for {
		err := upload(msg)
		if err == nil {
			return nil
		}
		if !config.enabled || !isOverloadError(err) {
			return err
		}
		log.Printf("Overload retry after %s: %v", backoff, err)
		sleep(backoff)
		if backoff < config.maxBackoff {
			backoff *= 2
			if backoff > config.maxBackoff {
				backoff = config.maxBackoff
			}
		}
	}
}

func runClientLoop(hammer bool, shouldStop func() bool, signalStop func(), doUpload func() error) error {
	for {
		if hammer && shouldStop != nil && shouldStop() {
			return nil
		}
		if err := doUpload(); err != nil {
			if isNoActiveEpochError(err) && signalStop != nil {
				signalStop()
			}
			return err
		}
		if !hammer {
			break
		}
	}
	return nil
}

func runClientWithStop(server string, nextMessage messageProvider, retryConfig overloadRetryConfig, shouldStop func() bool, signalStop func()) error {
	certs := make([]tls.Certificate, 1)
	certs[0] = utils.LeaderCertificate
	client, err := utils.DialHTTPWithTLS("tcp", server, -1, certs)
	if err != nil {
		return fmt.Errorf("could not connect: %w", err)
	}
	defer client.Close()

	//log.Printf("Connected")
	return runClientLoop(*hammerFlag, shouldStop, signalStop, func() error {
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
				return err
			}

		} else {
			msg, err := nextMessage()
			if err != nil {
				return err
			}
			err = uploadWithOverloadRetry(msg, retryConfig, func(msg *db.Plaintext) error {
				return tryUpload(client, msg)
			}, time.Sleep)
			if err != nil {
				log.Printf("Upload error: %v", err)
				return err
			}
		}

		return nil
	})
}

func resolveMessageProvider(x, y int, payload string) (messageProvider, error) {
	exactRequested := x >= 0 || y >= 0 || payload != ""
	if !exactRequested {
		return randomMessage, nil
	}
	if x < 0 || y < 0 || payload == "" {
		return nil, errors.New("must specify all of -x, -y, and -payload")
	}
	if x >= db.TABLE_WIDTH {
		return nil, fmt.Errorf("-x must be in [0,%d)", db.TABLE_WIDTH)
	}
	if y >= db.TABLE_HEIGHT {
		return nil, fmt.Errorf("-y must be in [0,%d)", db.TABLE_HEIGHT)
	}
	if len(payload) > db.SLOT_LENGTH {
		return nil, fmt.Errorf("-payload must be at most %d bytes", db.SLOT_LENGTH)
	}

	msg := new(db.Plaintext)
	msg.X = x
	msg.Y = y
	copy(msg.Message[:], []byte(payload))
	return func() (*db.Plaintext, error) {
		return msg, nil
	}, nil
}

func runClientWorker(target string, shouldStop func() bool, signalStop func()) error {
	retryConfig, err := resolveOverloadRetryConfig(*hammerFlag && *retryOverloadFlag, *overloadBackoffInitialFlag, *overloadBackoffMaxFlag)
	if err != nil {
		return err
	}
	if *donothingFlag {
		return runClientWithStop(target, nil, retryConfig, shouldStop, signalStop)
	}
	//log.Printf("=== Starting Client ===")
	nextMessage, err := resolveMessageProvider(*xFlag, *yFlag, *payloadFlag)
	if err != nil {
		return err
	}

	//log.Printf("Insert into [%v,%v]", xIdx, yIdx)
	//log.Printf("Plaintext [%v]", msg)
	return runClientWithStop(target, nextMessage, retryConfig, shouldStop, signalStop)
}

func runHammerClients(concurrent int, runOnce func(func() bool, func()) error) error {
	var stop atomic.Bool
	shouldStop := func() bool {
		return stop.Load()
	}
	signalStop := func() {
		stop.Store(true)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, concurrent)
	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func() {
			defer wg.Done()
			err := runOnce(shouldStop, signalStop)
			if isNoActiveEpochError(err) {
				signalStop()
			}
			errCh <- err
		}()
	}

	wg.Wait()
	close(errCh)
	for clientErr := range errCh {
		if clientErr == nil || isNoActiveEpochError(clientErr) {
			continue
		}
		return clientErr
	}
	return nil
}

func resolveHammerConcurrency(value uint) (int, error) {
	if value == 0 {
		return 0, errors.New("-concurrency must be positive")
	}
	return int(value), nil
}

func resolveOverloadRetryConfig(enabled bool, initialMS uint, maxMS uint) (overloadRetryConfig, error) {
	if !enabled {
		return overloadRetryConfig{}, nil
	}
	if initialMS == 0 {
		return overloadRetryConfig{}, errors.New("-overload-backoff-initial-ms must be positive")
	}
	if maxMS == 0 {
		return overloadRetryConfig{}, errors.New("-overload-backoff-max-ms must be positive")
	}
	initial := time.Duration(initialMS) * time.Millisecond
	maximum := time.Duration(maxMS) * time.Millisecond
	if maximum < initial {
		return overloadRetryConfig{}, errors.New("-overload-backoff-max-ms must be greater than or equal to -overload-backoff-initial-ms")
	}
	return overloadRetryConfig{
		enabled:        true,
		initialBackoff: initial,
		maxBackoff:     maximum,
	}, nil
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
		err = runClientWorker(target, nil, nil)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		// Make many requests concurrently
		concurrent, err := resolveHammerConcurrency(*concurrencyFlag)
		if err != nil {
			log.Fatal(err)
		}
		err = runHammerClients(
			concurrent,
			func(shouldStop func() bool, signalStop func()) error {
				return runClientWorker(target, shouldStop, signalStop)
			},
		)
		if err != nil {
			log.Fatal(err)
		}
	}
}
