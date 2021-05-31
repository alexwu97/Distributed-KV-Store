package distkvs

import (
	"fmt"
	"log"

	"example.org/cpsc416/a5/kvslib"
	"github.com/DistributedClocks/tracing"
)

const ChCapacity = 10

type ClientConfig struct {
	ClientID         string
	FrontEndAddr     string
	TracerServerAddr string
	TracerSecret     []byte
}

type Client struct {
	NotifyChannel kvslib.NotifyChannel
	id            string
	frontEndAddr  string
	kvs           *kvslib.KVS
	tracer        *tracing.Tracer
	initialized   bool
	tracerConfig  tracing.TracerConfig
}

func NewClient(config ClientConfig, kvs kvslib.KVS) *Client {
	return &Client{NotifyChannel: nil, id: config.ClientID, frontEndAddr: config.FrontEndAddr, kvs: &kvs, tracer: nil, initialized: false, tracerConfig: tracing.TracerConfig{}}
}

func (c *Client) Initialize() error {
	channel, err := c.kvs.Initialize(nil, c.id, c.frontEndAddr, 20)
	if err != nil {
		log.Fatal(err)
	}
	c.NotifyChannel = channel
	return nil
}

func (c *Client) Get(clientId string, key string) (uint32, error) {
	return c.kvs.Get(c.tracer, clientId, key)
}

func (c *Client) Put(clientId string, key string, value string) (uint32, error) {
	fmt.Println("Key: " + key + " Value: " + value)
	return c.kvs.Put(c.tracer, clientId, key, value)
}

func (c *Client) Close() error {
	return c.kvs.Close()
}
