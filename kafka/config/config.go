package config

type KafkaConfig struct {
	Brokers              []string
	User                 string
	Password             string
	TLS                  bool
	SASL                 bool
	Mechanism            string
	Auth                 bool
	ConsumerGroupVersion string
}
