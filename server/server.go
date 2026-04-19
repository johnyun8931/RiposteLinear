package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
)

import (
	"bitbucket.org/henrycg/riposte/db"
	"bitbucket.org/henrycg/riposte/utils"
)

var flagProfile = flag.Bool("profile", false, "Run CPU profiler")
var flagIndex = flag.Int("idx", -1, "Server index")
var flagLog = flag.String("log", "", "Log file")
var flagThreads = flag.Int("threads", -1, "Number of threads to use")
var flagResultsDir = flag.String("results-dir", "", "Directory for epoch result files on the leader")
var flagAdminTarget = flag.String("admin-target", "", "Target leader address for admin RPC commands")
var flagStartEpoch = flag.Int64("start-epoch-seconds", 0, "If set, issue an admin RPC to start an epoch for the given duration in seconds and exit")
var flagEpochStatus = flag.Bool("epoch-status", false, "If set, query leader epoch status over admin RPC and exit")

// List of server addresses
type serverListType []string

var serverList serverListType

func (s *serverListType) String() string {
	return fmt.Sprint(*s)
}

// Comma-separated list of server addresses (ip:port)
func (s *serverListType) Set(value string) error {
	if len(*s) > 0 {
		return errors.New("server flag already set")
	}

	for _, dt := range strings.Split(value, ",") {
		*s = append(*s, dt)
	}
	return nil
}

func init() {
	flag.Var(&serverList, "servers", "Comma-separated list of servers (in \"ip:port\" form)")
}

func main() {
	flag.Parse()

	if *flagStartEpoch > 0 || *flagEpochStatus {
		if *flagAdminTarget == "" {
			log.Fatal("Must specify -admin-target for admin RPC commands")
		}
		runAdminCommand()
		return
	}

	if *flagLog != "" {
		f, ferr := os.OpenFile(*flagLog, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)
		if ferr != nil {
			log.Fatal("Could not open log file ", *flagLog)
			os.Exit(1)
		}
		log.SetOutput(f)
	}

	if *flagIndex < 0 {
		log.Fatal("Must server index must be greater than zero")
		return
	}

	idx := *flagIndex

	if len(serverList) < 1 || idx > len(serverList)-1 {
		log.Fatal("Must specify a list of servers")
		return
	}

	if *flagThreads > 0 {
		runtime.GOMAXPROCS(int(*flagThreads))
	}

	defer log.Printf("Server died.")

	log.SetPrefix(fmt.Sprintf("[Server %v] ", idx))

	if *flagProfile {
		f, err := os.Create(fmt.Sprintf("server-%v.prof", idx))
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		// Stop when process exits
		defer pprof.StopCPUProfile()

		// Stop on ^C
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		signal.Notify(c, os.Kill)
		go func() {
			for _ = range c {
				// sig is a ^C, handle it
				pprof.StopCPUProfile()
				os.Exit(0)
			}
		}()
	}

	var a int
	slotTable := db.NewServer(idx, serverList)
	slotTable.SetResultsDir(*flagResultsDir)
	slotTable.Initialize(&a, &a)
	rpc.Register(slotTable)

	var certs []tls.Certificate

	// If we are not the leader, only allow
	// connections from the leader
	if idx > 0 {
		certs = append(certs, utils.LeaderCertificate)
	}

	utils.ListenAndServe(serverList[idx], idx, certs)
	log.Printf("Server %d is listening at %s", idx, serverList[idx])

	//http.ListenAndServe(addr, nil)
}

func runAdminCommand() {
	certs := make([]tls.Certificate, 1)
	certs[0] = utils.LeaderCertificate
	client, err := utils.DialHTTPWithTLS("tcp", *flagAdminTarget, -1, certs)
	if err != nil {
		log.Fatal("Could not connect to admin target: ", err)
	}
	defer client.Close()

	if *flagStartEpoch > 0 {
		var reply db.StartEpochReply
		err = client.Call("Server.StartEpoch", &db.StartEpochArgs{DurationSeconds: *flagStartEpoch}, &reply)
		if err != nil {
			log.Fatal("Could not start epoch: ", err)
		}
		log.Printf("started epoch=%d state=%s start=%d end=%d duration=%d", reply.EpochID, reply.State, reply.StartUnix, reply.EndUnix, reply.DurationSecs)
		return
	}

	if *flagEpochStatus {
		var reply db.EpochStatusReply
		err = client.Call("Server.EpochStatus", &db.EpochStatusArgs{}, &reply)
		if err != nil {
			log.Fatal("Could not query epoch status: ", err)
		}
		log.Printf("epoch=%d state=%s accepting=%t start=%d end=%d duration=%d last_result=%s", reply.EpochID, reply.State, reply.Accepting, reply.StartUnix, reply.EndUnix, reply.DurationSecs, reply.LastResult)
	}
}
