package kafka

import (
	"crypto/sha512"

	"github.com/Shopify/sarama"
	"github.com/xdg/scram"
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

func Config(user, password, mechanism string, auth, SASL, TLS bool) *sarama.Config {
	cfg := sarama.NewConfig()
	if !auth {
		return cfg
	}
	if SASL {
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: sha512.New} }
	}
	cfg.Net.TLS.Enable = TLS
	cfg.Net.SASL.Enable = SASL
	cfg.Net.SASL.Mechanism = sarama.SASLMechanism(mechanism)
	cfg.Net.SASL.User = user
	cfg.Net.SASL.Password = password
	return cfg
}
