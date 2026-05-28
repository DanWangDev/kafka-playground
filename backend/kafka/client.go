package kafka

// BrokerAddresses are the bootstrap servers for the 3-node KRaft cluster.
// The Go backend runs natively on the host and connects via localhost ports.
var BrokerAddresses = []string{
	"localhost:9092",
	"localhost:9093",
	"localhost:9094",
}
