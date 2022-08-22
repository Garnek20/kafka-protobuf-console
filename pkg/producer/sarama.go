package producer

import (
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/khorshuheng/kafka-protobuf-console/pkg/utils"
)

type SaramaProducer struct {
	saramaClient sarama.SyncProducer
}

// keySize size in bytes of generated key
const keySize = 7

func NewSaramaProducer(brokers []string) (SaramaProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	client, err := sarama.NewSyncProducer(brokers, cfg)

	return SaramaProducer{client}, err
}

func (p SaramaProducer) Send(topic string, msg proto.Message) error {
	msgByte, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	encodedMsgByte := sarama.ByteEncoder(msgByte)

	pmsg := &sarama.ProducerMessage{
		Key:   sarama.StringEncoder(utils.RandStringBytes(keySize)),
		Topic: topic,
		Value: encodedMsgByte,
	}

	_, _, err = p.saramaClient.SendMessage(pmsg)
	return err
}
