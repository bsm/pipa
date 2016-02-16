package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/pipa"
	"github.com/bsm/sarama-cluster"
)

var (
	groupID    = flag.String("group", "", "REQUIRED: The shared consumer group name")
	brokerList = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topicList  = flag.String("topics", "", "REQUIRED: The comma separated list of topics to consume")

	logger    = log.New(os.Stdout, "[pipa] ", log.LstdFlags)
	stdPolicy = pipa.RetryPolicy{Times: 2, Sleep: time.Second}
)

func main() {
	flag.Parse()

	// validate config
	if *groupID == "" {
		logger.Fatal("you have to provide a -group name.")
		return
	} else if *brokerList == "" {
		logger.Fatal("you have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
		return
	} else if *topicList == "" {
		logger.Fatal("you have to provide -topics as a comma-separated list.")
		return
	}

	// create notifier
	notifier := pipa.NewStdLogNotifier(logger)

	// connect consumer
	consumer, err := pipa.NewConsumer(
		strings.Split(*brokerList, ","),
		*groupID,
		strings.Split(*topicList, ","),
		clusterConfig(),
		notifier,
	)
	if err != nil {
		logger.Fatalf("unable to connect consumer: %s", err.Error())
		return
	}
	defer consumer.Close()

	// process in the background
	go pipa.NewInputStream(consumer, notifier).
		Parse(10, jsonParser{}).
		Batch(time.Second).
		Process(stdoutHandler{})

	// wait for stop signal
	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-wait
}

// --------------------------------------------------------------------

func clusterConfig() *cluster.Config {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	return config
}

// --------------------------------------------------------------------

type jsonParser struct{}

func (p jsonParser) Policy() pipa.RetryPolicy { return stdPolicy }
func (p jsonParser) Parse(m *sarama.ConsumerMessage) (interface{}, error) {
	var v map[string]interface{}
	if err := json.Unmarshal(m.Value, &v); err != nil {
		return nil, err
	}
	return v, nil
}

type stdoutHandler struct{}

func (h stdoutHandler) Name() string             { return "cli-example" }
func (h stdoutHandler) Policy() pipa.RetryPolicy { return stdPolicy }
func (h stdoutHandler) Process(batch pipa.EventBatch) (n int, _ error) {
	for _, event := range batch {
		fmt.Fprintln(os.Stdout, event.Value)
		n++
	}
	return
}
