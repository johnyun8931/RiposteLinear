package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"bitbucket.org/henrycg/riposte/controlstore"
)

var flagCoordinator = flag.String("coordinator", "", "Coordinator admin RPC address")
var flagControlTable = flag.String("control-table", "", "DynamoDB control table name")
var flagAWSRegion = flag.String("aws-region", "", "AWS region override")
var flagIntervalSeconds = flag.Int("interval-seconds", 10, "Autoscaler evaluation interval in seconds")
var flagOnce = flag.Bool("once", false, "Run one evaluation cycle and exit")
var flagApply = flag.Bool("apply", false, "Apply applicable recommendations; default is dry-run only")
var flagMinRecommendationEpoch = flag.Int64("min-recommendation-epoch", 0, "Ignore recommendations older than this epoch")
var flagLog = flag.String("log", "", "Log file")

func main() {
	flag.Parse()
	if err := validateFlags(*flagCoordinator, *flagControlTable, *flagIntervalSeconds); err != nil {
		log.Fatal(err)
	}
	if *flagLog != "" {
		f, err := os.OpenFile(*flagLog, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)
		if err != nil {
			log.Fatal("Could not open log file ", *flagLog)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	options := []func(*awsconfig.LoadOptions) error{}
	if *flagAWSRegion != "" {
		options = append(options, awsconfig.WithRegion(*flagAWSRegion))
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), options...)
	if err != nil {
		log.Fatal(err)
	}

	store, err := controlstore.NewDynamoDBStore(dynamodb.NewFromConfig(cfg), *flagControlTable)
	if err != nil {
		log.Fatal(err)
	}

	scaler := autoscaler{
		control: store,
		coordinator: rpcCoordinatorAdmin{
			addr: *flagCoordinator,
		},
		apply:                  *flagApply,
		minRecommendationEpoch: *flagMinRecommendationEpoch,
		logger:                 log.Default(),
	}
	interval := time.Duration(*flagIntervalSeconds) * time.Second
	if err := scaler.run(context.Background(), interval, *flagOnce); err != nil {
		log.Fatal(err)
	}
}

func validateFlags(coordinator string, controlTable string, intervalSeconds int) error {
	if coordinator == "" {
		return errors.New("-coordinator is required")
	}
	if controlTable == "" {
		return errors.New("-control-table is required")
	}
	if intervalSeconds <= 0 {
		return fmt.Errorf("-interval-seconds must be positive")
	}
	return nil
}
