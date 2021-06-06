package distkvs

import (
	"container/list"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
	concurmap "github.com/orcaman/concurrent-map"
)

type StorageAddr string

// this matches the config file format in config/frontend_config.json
type FrontEndConfig struct {
	ClientAPIListenAddr  string
	StorageAPIListenAddr string
	Storage              StorageAddr
	TracerServerAddr     string
	TracerSecret         []byte
}

type FrontEndPut struct {
	Key   string
	Value string
}

type FrontEndGet struct {
	Key string
}

type FrontEndRequest struct {
	RequestType     string
	Key             string
	Value           string
	ClientId        string
	opID            uint32
	ResponseChannel chan FrontEndResult
}

type FrontEndResult struct {
	ClientId    string
	opID        uint32
	Key         string
	Value       string
	Err         bool
	RequestType string
}

type StorageJoinRequest struct {
	StorageId      string
	StorageAddress string
}

type StorageJoinReply struct {
	JoinState  string
	StorageMap map[string]StorageNode
}

type StorageUpToDateRequest struct {
	StorageId      string
	StorageAddress string
	JoinState      string
}

type StorageUpToDateResponse struct {
	success bool
}

type FrontEnd struct {
	// state may go here
	clientMap       concurmap.ConcurrentMap
	threadMap       concurmap.ConcurrentMap
	opIdMap         concurmap.ConcurrentMap
	keyMap          concurmap.ConcurrentMap
	storageMap      concurmap.ConcurrentMap
	storageConnMap  concurmap.ConcurrentMap
	storageConnCMap concurmap.ConcurrentMap
	storageQueue    *list.List
	queueMu         sync.Mutex
}

type StorageNode struct {
	JoinState      string
	StorageAddress string
}

type StorageConn struct {
	StorageId string
	client    *rpc.Client
}

var (
	timeOutPeriod int
	wgF           sync.WaitGroup
)

func (f *FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	f.storageMap = concurmap.New()
	f.storageConnMap = concurmap.New()
	f.storageConnCMap = concurmap.New()
	f.clientMap = concurmap.New()
	f.keyMap = concurmap.New()
	f.opIdMap = concurmap.New()
	f.threadMap = concurmap.New()
	f.storageQueue = list.New()
	timeOutPeriod = int(storageTimeout)

	//set up RPC handler and register the functions
	rpc.Register(f)
	//start listening for client and storage nodes
	wgF.Add(1)
	go f.StartListeningOnTcpPort(clientAPIListenAddr)
	wgF.Add(1)
	go f.StartListeningOnTcpPort(storageAPIListenAddr)
	wgF.Wait()
	return nil
}
