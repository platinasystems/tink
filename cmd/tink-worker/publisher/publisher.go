package publisher

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/platinasystems/go-common/v2/routine"
	"github.com/platinasystems/go-kafka-avro"
	avro2 "github.com/platinasystems/pcc-models/v2/bare_metal/avro"
	"os/exec"
	"time"
)

var (
	producer *kafka.AvroProducer
	Config   *Configuration
)

type Configuration struct {
	Topic          string
	SchemaRegistry string
	Broker         string
	Host           string
	TargetHost     string
	Period         int
}

type Publisher struct {
}

func Init(config *Configuration) {
	Config = config
	cmd := fmt.Sprintf("echo '%s kafka' >> /etc/hosts", config.TargetHost)
	if _, err := exec.Command("sh", "-c", cmd).Output(); err != nil {
		fmt.Println(fmt.Sprintf("unable to set kafka address to /etc/hosts: %v", err))
	}
	routine.Go(func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				LogQueue.SendAndFlush()
			}
		}
	}, nil)
}

func (p *Publisher) Send(schema string, jsonValue []byte, timestamp int64) error {

	if Config == nil {
		return errors.New("missing publisher configuration")
	}

	if producer == nil {
		config := sarama.NewConfig()
		config.Producer.Partitioner = sarama.NewRandomPartitioner
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.MaxMessageBytes = 15728640

		prod, prodError := kafka.NewAvroProducerCustomConfig(
			[]string{Config.Broker},
			[]string{Config.SchemaRegistry},
			config)
		if prodError != nil {
			fmt.Println(fmt.Sprintf("Unable to connect to kafka: [%+v]", prodError))
			//_ = log.AuctaLogger.Warnf("Unable to connect to kafka: [%+v]", prodError)
			return prodError
		}
		producer = prod
	}
	partition, offset, err := producer.AddWithResponse(Config.Topic, schema, []byte(fmt.Sprintf("%v", timestamp)), jsonValue)
	if err != nil {
		fmt.Println(fmt.Sprintf("Unable to push [%+v]. Error is: [%+v]", schema, err))
	} else {
		fmt.Println(fmt.Sprintf("Data has been pushed on topic [%s] to partition [%d] with offset [%d]", Config.Topic, partition, offset))
	}
	return err
}

func (p *Publisher) SendLogs(provisionerLogs []avro2.Log) (err error) {
	pl := avro2.NewProvisionerLogs()
	pl.Logs = provisionerLogs
	var js []byte
	if js, err = json.Marshal(pl); err != nil {
		fmt.Println(fmt.Sprintf("unable to serialize %v: %v", pl, err))
	} else {
		if err = p.Send(pl.Schema(), js, time.Now().Unix()); err != nil {
			fmt.Println(fmt.Sprintf("unable to send data %v: %v", pl, err))
		}
	}
	return
}
