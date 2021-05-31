package distkvs

import (
	"container/heap"
	"container/list"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"time"

	kvslib "example.org/cpsc416/a5/kvslib"
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

type FrontEndStorageStarted struct{}

type FrontEndStorageFailed struct{}

type FrontEndPut struct {
	Key   string
	Value string
}

type FrontEndPutResult struct {
	Key      string
	Value    *string
	Err      bool
	ClientId string
	OpId     uint32
}

type FrontEndGet struct {
	Key string
}

type FrontEndGetResult struct {
	Key      string
	Value    *string
	Err      bool
	ClientId string
	OpId     uint32
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

type FrontEnd struct {
	// state may go here
	clientMap concurmap.ConcurrentMap
	threadMap concurmap.ConcurrentMap
	opIdMap   concurmap.ConcurrentMap
	keyMap    concurmap.ConcurrentMap
	client    *rpc.Client
}

var (
	timeOutPeriod        int
	goroutineCounter     int
	clientroutinecounter int
)

func (f *FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	f.clientMap = concurmap.New()
	f.keyMap = concurmap.New()
	f.opIdMap = concurmap.New()
	f.threadMap = concurmap.New()
	timeOutPeriod = int(storageTimeout)

	client, err := rpc.Dial("tcp", "localhost"+storageAPIListenAddr)
	if err != nil {
		log.Fatal("cannot rpc connect to Storage")
	}
	defer client.Close()
	f.client = client
	//set up RPC handler and register the functions
	rpc.Register(f)
	rpc.HandleHTTP()
	http.ListenAndServe(clientAPIListenAddr, nil)

	return nil
}

func (f *FrontEnd) EstablishResourceForClient(clientId *string, reply *kvslib.KVSResponseInitialize) error {
	prioQueue := make(PriorityQueue, 0)
	f.clientMap.SetIfAbsent(*clientId, &prioQueue)
	f.opIdMap.SetIfAbsent(*clientId, uint32(0))

	reply.Err = false
	return nil
}

func (f *FrontEnd) HandleReadRequest(clientGetRequest *kvslib.KvslibGet, reply *kvslib.KvslibGetResult) error {
	newGetChannel := make(chan FrontEndResult, 1)
	requestItem := &Item{ClientId: clientGetRequest.ClientId, OpId: clientGetRequest.OpId, Key: clientGetRequest.Key, Request: "get", ResponseChannel: newGetChannel}
	//get the priority queue
	pq, _ := f.clientMap.Get(clientGetRequest.ClientId)
	heap.Push(pq.(*PriorityQueue), requestItem)

	ok := f.threadMap.SetIfAbsent(clientGetRequest.ClientId, true)
	if ok {
		clientroutinecounter++
		go f.sendRequestsToKeyMap(clientGetRequest.ClientId)
	}

	result := <-newGetChannel
	reply.Err = result.Err
	reply.Key = result.Key
	reply.ClientId = result.ClientId
	reply.OpId = result.opID
	reply.Value = &result.Value
	return nil
}

func (f *FrontEnd) HandlePutRequest(clientPutRequest *kvslib.KvslibPut, reply *kvslib.KvslibPutResult) error {
	newPutChannel := make(chan FrontEndResult, 1)
	requestItem := &Item{ClientId: clientPutRequest.ClientId, OpId: clientPutRequest.OpId, Key: clientPutRequest.Key, Value: clientPutRequest.Value, Request: "put", ResponseChannel: newPutChannel}
	//get the priority queue
	pq, _ := f.clientMap.Get(clientPutRequest.ClientId)
	heap.Push(pq.(*PriorityQueue), requestItem)
	ok := f.threadMap.SetIfAbsent(clientPutRequest.ClientId, true)
	if ok {
		clientroutinecounter++
		go f.sendRequestsToKeyMap(clientPutRequest.ClientId)
	}

	result := <-newPutChannel
	reply.Err = result.Err
	reply.Key = result.Key
	reply.ClientId = result.ClientId
	reply.OpId = result.opID
	reply.Value = &result.Value

	return nil
}

func (f *FrontEnd) sendRequestsToKeyMap(clientId string) {
	defer f.threadMap.Remove(clientId)
	for first, _ := f.clientMap.Get(clientId); first.(*PriorityQueue).Len() > 0; {
		fmt.Println("client: " + fmt.Sprint(clientroutinecounter))
		//fmt.Println("pq size on right: " + fmt.Sprint(first.(*PriorityQueue).Len()) + " goroutine: " + fmt.Sprint(goroutineCounter))
		firstItem := first.(*PriorityQueue).Peek().(*Item)
		if clientOpId, _ := f.opIdMap.Get(clientId); firstItem.OpId == clientOpId.(uint32) {
			item := heap.Pop(first.(*PriorityQueue)).(*Item)
			frontEndRequest := FrontEndRequest{RequestType: item.Request, Key: item.Key, Value: item.Value, ClientId: item.ClientId, opID: item.OpId, ResponseChannel: item.ResponseChannel}
			f.keyMap.SetIfAbsent(item.Key, list.New())
			queue, _ := f.keyMap.Get(item.Key)
			queue.(*list.List).PushBack(frontEndRequest)
			f.opIdMap.Set(clientId, clientOpId.(uint32)+1)

			ok := f.threadMap.SetIfAbsent(item.Key, true)
			if ok {
				goroutineCounter++
				go f.sendRequestToStorage(item.Key)
			}
		}
	}
	clientroutinecounter--
}

func (f *FrontEnd) sendRequestToStorage(key string) {
	defer f.threadMap.Remove(key)
	for keyQueue, _ := f.keyMap.Get(key); keyQueue.(*list.List).Len() > 0; {
		fmt.Println(goroutineCounter)
		frontEle := keyQueue.(*list.List).Front()
		keyQueue.(*list.List).Remove(frontEle)
		request := FrontEndRequest(frontEle.Value.(FrontEndRequest))

		if request.RequestType == "get" {
			getRequest := FrontEndGet{Key: request.Key}
			var reply StorageGetResult
			var getResult FrontEndResult
			gettingRequest := f.client.Go("Storage.GetStorage", getRequest, &reply, nil)
			select {
			case res := <-gettingRequest.Done:
				resReply := res.Reply.(*StorageGetResult)
				getResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: resReply.Key, RequestType: request.RequestType}
				if res.Error != nil {
					getResult.Err = true
				} else {
					getResult.Err = false
					getResult.Value = *resReply.Value
				}
			case <-time.After(time.Duration(timeOutPeriod) * time.Second):
				temp := f.client.Go("Storage.GetStorage", getRequest, &reply, nil)
				select {
				case res := <-temp.Done:
					resReply := res.Reply.(*StorageGetResult)
					getResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: resReply.Key, RequestType: request.RequestType}
					if res.Error != nil {
						getResult.Err = true
					} else {
						getResult.Err = false
						getResult.Value = *resReply.Value
					}
				case <-time.After(time.Duration(timeOutPeriod) * time.Second):
					getResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: request.Key, RequestType: request.RequestType, Err: true}
				}
			}
			returnChan := request.ResponseChannel
			returnChan <- getResult
		} else {
			putRequest := FrontEndPut{Key: request.Key, Value: request.Value}
			var reply StorageGetResult
			var putResult FrontEndResult
			puttingRequest := f.client.Go("Storage.PutStorage", putRequest, &reply, nil)
			select {
			case res := <-puttingRequest.Done:
				resReply := res.Reply.(*StorageGetResult)
				putResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: resReply.Key, Value: *resReply.Value, RequestType: request.RequestType}
				if res.Error != nil {
					putResult.Err = true
				} else {
					putResult.Err = false
				}
			case <-time.After(time.Duration(timeOutPeriod) * time.Second):
				//close(puttingRequest.Done)
				temp := f.client.Go("Storage.PutStorage", putRequest, &reply, nil)
				select {
				case res := <-temp.Done:
					resReply := res.Reply.(*StorageGetResult)
					putResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: resReply.Key, Value: *resReply.Value, RequestType: request.RequestType}
					if res.Error != nil {
						putResult.Err = true
					} else {
						putResult.Err = false
					}
				case <-time.After(time.Duration(timeOutPeriod) * time.Second):
					putResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: request.Key, RequestType: request.RequestType, Err: true}
				}
			}
			returnChan := request.ResponseChannel
			returnChan <- putResult
		}
	}

	goroutineCounter--
	if key == "5" {
		fmt.Println("deleting")
	}
}
