package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"

	"bitbucket.org/henrycg/riposte/db"
	"bitbucket.org/henrycg/riposte/utils"
)

var flagListen = flag.String("listen", "", "Coordinator listen address")
var flagLog = flag.String("log", "", "Log file")
var flagAdminTarget = flag.String("admin-target", "", "Target coordinator address for admin RPC commands")
var flagStartEpoch = flag.Int64("start-epoch-seconds", 0, "If set, issue an admin RPC to start an epoch for the given duration in seconds and exit")
var flagEpochStatus = flag.Bool("epoch-status", false, "If set, query coordinator epoch status over admin RPC and exit")
var flagStatus = flag.Bool("status", false, "If set, query coordinator status over admin RPC and exit")

var shardFlags shardListType

func init() {
	flag.Var(&shardFlags, "shard", "Static shard config: id,start,end,activeLeader,activeFollower[,standbyLeader|standbyFollower]")
}

func main() {
	flag.Parse()

	if *flagStartEpoch > 0 || *flagEpochStatus || *flagStatus {
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
		}
		log.SetOutput(f)
	}
	if *flagListen == "" {
		log.Fatal("Must specify -listen")
	}
	if len(shardFlags) == 0 {
		log.Fatal("Must configure at least one -shard")
	}

	shards := make([]ShardConfig, 0, len(shardFlags))
	for _, raw := range shardFlags {
		shard, err := parseShardConfig(raw)
		if err != nil {
			log.Fatal(err)
		}
		shards = append(shards, shard)
	}

	coord, err := NewCoordinator(shards, nil)
	if err != nil {
		log.Fatal(err)
	}
	if err := coord.connectShards(); err != nil {
		log.Fatal(err)
	}

	if err := rpc.RegisterName("Server", coord); err != nil {
		log.Fatal(err)
	}
	var certs []tls.Certificate
	utils.ListenAndServe(*flagListen, 0, certs)
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
		return
	}

	if *flagStatus {
		var reply db.CoordinatorStatusReply
		err = client.Call("Server.Status", &db.CoordinatorStatusArgs{}, &reply)
		if err != nil {
			log.Fatal("Could not query status: ", err)
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(reply); err != nil {
			log.Fatal("Could not encode status: ", err)
		}
		return
	}
	log.Fatal(errors.New("no admin command requested"))
}

func ExampleShardFlag() string {
	return strings.Join([]string{
		"0,0,128,127.0.0.1:8090,127.0.0.1:8091",
		"1,128,256,127.0.0.1:8190,127.0.0.1:8191",
		"2,256,384,127.0.0.1:8290,127.0.0.1:8291,127.0.0.1:8390|127.0.0.1:8391",
	}, "\n")
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintln(flag.CommandLine.Output(), "\nExample shard configs:")
		fmt.Fprintln(flag.CommandLine.Output(), ExampleShardFlag())
	}
}
