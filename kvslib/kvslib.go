// Package kvslib provides an API which is a wrapper around RPC calls to the
// frontend.
package kvslib

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type KvslibBegin struct {
	ClientId string
}

type KvslibPut struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type KvslibGet struct {
	ClientId string
	OpId     uint32
	Key      string
}

type KvslibPutResult struct {
	Key      string
	Value    *string
	Err      bool
	ClientId string
	OpId     uint32
}

type KvslibGetResult struct {
	Key      string
	Value    *string
	Err      bool
	ClientId string
	OpId     uint32
}

type KvslibResult struct {
	Key      string
	Value    *string
	Err      bool
	ClientId string
	OpId     uint32
}

type KvslibComplete struct {
	ClientId string
}

type KVSResponseInitialize struct {
	Err bool
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId        uint32
	StorageFail bool
	Result      string
}

type KVS struct {
	notifyCh NotifyChannel

	// Add more KVS instance state here.
	client *rpc.Client
	OpId   uint32
	mu     *sync.Mutex
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
		OpId:     0,
		mu:       &sync.Mutex{},
	}
}

// Initialize Initializes the instance of KVS to use for connecting to the frontend,
// and the frontends IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by kvslib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Initialize(localTracer *tracing.Tracer, clientId string, frontEndAddr string, chCapacity uint) (NotifyChannel, error) {
	d.notifyCh = make(chan ResultStruct, chCapacity)
	client, err := rpc.Dial("tcp", "localhost"+frontEndAddr)
	if err != nil {
		log.Fatal("cannot rpc connect to FrontEnd")
	}
	d.client = client
	var reply KVSResponseInitialize
	d.client.Call("FrontEnd.EstablishResourceForClient", clientId, &reply)
	if reply.Err {
		log.Fatal("cannot set up clientID at FrontEnd")
	}
	return d.notifyCh, nil
}

// Get is a non-blocking request from the client to the system. This call is used by
// the client when it wants to get value for a key.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	// Should return OpId or error
	d.mu.Lock()
	args := KvslibGet{ClientId: clientId, OpId: d.OpId, Key: key}
	d.OpId++
	d.mu.Unlock()
	var reply KvslibGetResult
	divCall := d.client.Go("FrontEnd.HandleReadRequest", args, &reply, nil)
	replyCall := <-divCall.Done
	res := replyCall.Reply.(*KvslibGetResult)
	resStr := ResultStruct{OpId: res.OpId, StorageFail: res.Err, Result: *res.Value}
	d.notifyCh <- resStr
	return reply.OpId, nil
}

// Put is a non-blocking request from the client to the system. This call is used by
// the client when it wants to update the value of an existing key or add add a new
// key and value pair.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	// Should return OpId or error
	d.mu.Lock()
	args := KvslibPut{ClientId: clientId, OpId: d.OpId, Key: key, Value: value}
	d.OpId++
	d.mu.Unlock()
	var reply KvslibPutResult
	divCall := d.client.Go("FrontEnd.HandlePutRequest", args, &reply, nil)
	replyCall := <-divCall.Done
	res := replyCall.Reply.(*KvslibPutResult)
	resStr := ResultStruct{OpId: res.OpId, StorageFail: res.Err, Result: *res.Value}
	fmt.Println(resStr.Result)
	d.notifyCh <- resStr
	return reply.OpId, nil
}

// Close Stops the KVS instance from communicating with the frontend and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *KVS) Close() error {
	d.client.Close()
	return nil
}
