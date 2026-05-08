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

type uploadRequest struct {
	msg      *db.Plaintext
	routeRow int
}

type messageProvider func() (uploadRequest, error)

type overloadRetryConfig struct {
	enabled        bool
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

type coordinatorRetryConfig struct {
	enabled        bool
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

type rpcUploader struct {
	server string
	client *rpc.Client
}

func (u *rpcUploader) connect() error {
	if u.client != nil {
		return nil
	}
	certs := make([]tls.Certificate, 1)
	certs[0] = utils.LeaderCertificate
	client, err := utils.DialHTTPWithTLS("tcp", u.server, -1, certs)
	if err != nil {
		return fmt.Errorf("could not connect: %w", err)
	}
	u.client = client
	return nil
}

func (u *rpcUploader) close() {
	if u.client == nil {
		return
	}
	if err := u.client.Close(); err != nil {
		log.Printf("Close client connection: %v", err)
	}
	u.client = nil
}

func (u *rpcUploader) upload(req uploadRequest) error {
	if err := u.connect(); err != nil {
		return err
	}
	return tryUpload(u.client, req)
}

func tryUpload(client *rpc.Client, req uploadRequest) error {
	var upRes1 db.UploadReply1
	var upArgs1 db.UploadArgs1

	msgBitShares, err := db.InitializeUploadArgs(&upArgs1, req.msg, *bogusFlag)
	if err != nil {
		panic("Error initializing upload args")
	}
	upArgs1.RouteRow = req.routeRow

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
	upArgs3 := db.SetUploadArgs3(req.msg, mint, &upArgs1, &upRes1, upArgs2, &upRes2)

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

func isCoordinatorNotActiveError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Coordinator not active")
}

func isOverloadError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "server overloaded: ready queue full")
}

func nextBackoff(backoff time.Duration, maximum time.Duration) time.Duration {
	if backoff < maximum {
		backoff *= 2
		if backoff > maximum {
			backoff = maximum
		}
	}
	return backoff
}

func uploadWithOverloadRetry(req uploadRequest, config overloadRetryConfig, upload func(uploadRequest) error, sleep func(time.Duration)) error {
	backoff := config.initialBackoff
	for {
		err := upload(req)
		if err == nil {
			return nil
		}
		if !config.enabled || !isOverloadError(err) {
			return err
		}
		log.Printf("Overload retry after %s: %v", backoff, err)
		sleep(backoff)
		backoff = nextBackoff(backoff, config.maxBackoff)
	}
}

func uploadWithCoordinatorRetry(req uploadRequest, config coordinatorRetryConfig, upload func(uploadRequest) error, sleep func(time.Duration)) error {
	backoff := config.initialBackoff
	for {
		err := upload(req)
		if err == nil {
			return nil
		}
		if !config.enabled || !isCoordinatorNotActiveError(err) {
			return err
		}
		log.Printf("Coordinator retry after %s: %v", backoff, err)
		sleep(backoff)
		backoff = nextBackoff(backoff, config.maxBackoff)
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

func runClientWithStop(server string, nextMessage messageProvider, overloadConfig overloadRetryConfig, coordinatorConfig coordinatorRetryConfig, shouldStop func() bool, signalStop func()) error {
	uploader := &rpcUploader{server: server}
	defer uploader.close()

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
			if err := uploader.connect(); err != nil {
				return err
			}
			if err := uploader.client.Call("Server.DoNothing", &a, &b); err != nil {
				return err
			}

		} else {
			req, err := nextMessage()
			if err != nil {
				return err
			}
			err = uploadWithCoordinatorRetry(req, coordinatorConfig, func(req uploadRequest) error {
				return uploadWithOverloadRetry(req, overloadConfig, func(req uploadRequest) error {
					err := uploader.upload(req)
					if coordinatorConfig.enabled && isCoordinatorNotActiveError(err) {
						uploader.close()
					}
					return err
				}, time.Sleep)
			}, time.Sleep)
			if err != nil {
				log.Printf("Upload error: %v", err)
				return err
			}
		}

		return nil
	})
}

func randomLocalUploadRequest() (uploadRequest, error) {
	msg, err := randomMessage()
	if err != nil {
		return uploadRequest{}, err
	}
	return uploadRequest{msg: msg, routeRow: msg.Y}, nil
}

func randomCoordinatorUploadRequest(globalTableHeight int) (uploadRequest, error) {
	if globalTableHeight <= 0 {
		return uploadRequest{}, errors.New("global table height must be positive")
	}
	msg := new(db.Plaintext)
	msg.X = utils.RandIntShort(db.TABLE_WIDTH)
	globalRow := utils.RandIntShort(globalTableHeight)
	msg.Y = globalRow % db.TABLE_HEIGHT
	if err := db.RandomSlot(&msg.Message); err != nil {
		return uploadRequest{}, err
	}
	return uploadRequest{msg: msg, routeRow: globalRow}, nil
}

func resolveMessageProvider(x, y int, payload string, globalTableHeight int) (messageProvider, error) {
	exactRequested := x >= 0 || y >= 0 || payload != ""
	if !exactRequested {
		if globalTableHeight > db.TABLE_HEIGHT {
			return func() (uploadRequest, error) {
				return randomCoordinatorUploadRequest(globalTableHeight)
			}, nil
		}
		return randomLocalUploadRequest, nil
	}
	if x < 0 || y < 0 || payload == "" {
		return nil, errors.New("must specify all of -x, -y, and -payload")
	}
	if x >= db.TABLE_WIDTH {
		return nil, fmt.Errorf("-x must be in [0,%d)", db.TABLE_WIDTH)
	}
	rowLimit := db.TABLE_HEIGHT
	if globalTableHeight > rowLimit {
		rowLimit = globalTableHeight
	}
	if y >= rowLimit {
		return nil, fmt.Errorf("-y must be in [0,%d)", rowLimit)
	}
	if len(payload) > db.SLOT_LENGTH {
		return nil, fmt.Errorf("-payload must be at most %d bytes", db.SLOT_LENGTH)
	}

	msg := new(db.Plaintext)
	msg.X = x
	msg.Y = y % db.TABLE_HEIGHT
	copy(msg.Message[:], []byte(payload))
	return func() (uploadRequest, error) {
		return uploadRequest{msg: msg, routeRow: y}, nil
	}, nil
}

func queryCoordinatorGlobalTableHeight(target string) (int, error) {
	certs := make([]tls.Certificate, 1)
	certs[0] = utils.LeaderCertificate
	client, err := utils.DialHTTPWithTLS("tcp", target, -1, certs)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	var reply db.CoordinatorStatusReply
	if err := client.Call("Server.Status", &db.CoordinatorStatusArgs{}, &reply); err != nil {
		return 0, err
	}
	return reply.GlobalTableHeight, nil
}

func runClientWorker(target string, globalTableHeight int, shouldStop func() bool, signalStop func()) error {
	overloadEnabled := *hammerFlag && *retryOverloadFlag
	coordinatorRetryEnabled := *coordinatorFlag != ""
	backoffConfig, err := resolveBackoffConfig(overloadEnabled || coordinatorRetryEnabled, *overloadBackoffInitialFlag, *overloadBackoffMaxFlag)
	if err != nil {
		return err
	}
	overloadConfig := overloadRetryConfig{
		enabled:        overloadEnabled,
		initialBackoff: backoffConfig.initialBackoff,
		maxBackoff:     backoffConfig.maxBackoff,
	}
	coordinatorConfig := coordinatorRetryConfig{
		enabled:        coordinatorRetryEnabled,
		initialBackoff: backoffConfig.initialBackoff,
		maxBackoff:     backoffConfig.maxBackoff,
	}
	if *donothingFlag {
		return runClientWithStop(target, nil, overloadConfig, coordinatorConfig, shouldStop, signalStop)
	}
	//log.Printf("=== Starting Client ===")
	nextMessage, err := resolveMessageProvider(*xFlag, *yFlag, *payloadFlag, globalTableHeight)
	if err != nil {
		return err
	}

	//log.Printf("Insert into [%v,%v]", xIdx, yIdx)
	//log.Printf("Plaintext [%v]", msg)
	return runClientWithStop(target, nextMessage, overloadConfig, coordinatorConfig, shouldStop, signalStop)
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

func resolveBackoffConfig(enabled bool, initialMS uint, maxMS uint) (coordinatorRetryConfig, error) {
	if !enabled {
		return coordinatorRetryConfig{}, nil
	}
	if initialMS == 0 {
		return coordinatorRetryConfig{}, errors.New("-overload-backoff-initial-ms must be positive")
	}
	if maxMS == 0 {
		return coordinatorRetryConfig{}, errors.New("-overload-backoff-max-ms must be positive")
	}
	initial := time.Duration(initialMS) * time.Millisecond
	maximum := time.Duration(maxMS) * time.Millisecond
	if maximum < initial {
		return coordinatorRetryConfig{}, errors.New("-overload-backoff-max-ms must be greater than or equal to -overload-backoff-initial-ms")
	}
	return coordinatorRetryConfig{
		initialBackoff: initial,
		maxBackoff:     maximum,
	}, nil
}

func resolveOverloadRetryConfig(enabled bool, initialMS uint, maxMS uint) (overloadRetryConfig, error) {
	backoffConfig, err := resolveBackoffConfig(enabled, initialMS, maxMS)
	if err != nil {
		return overloadRetryConfig{}, err
	}
	return overloadRetryConfig{
		enabled:        enabled,
		initialBackoff: backoffConfig.initialBackoff,
		maxBackoff:     backoffConfig.maxBackoff,
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

	globalTableHeight := db.TABLE_HEIGHT
	if *coordinatorFlag != "" {
		globalTableHeight, err = queryCoordinatorGlobalTableHeight(target)
		if err != nil {
			log.Fatal("Could not query coordinator status: ", err)
		}
	}

	// Make one request
	if !*hammerFlag {
		err = runClientWorker(target, globalTableHeight, nil, nil)
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
				return runClientWorker(target, globalTableHeight, shouldStop, signalStop)
			},
		)
		if err != nil {
			log.Fatal(err)
		}
	}
}
