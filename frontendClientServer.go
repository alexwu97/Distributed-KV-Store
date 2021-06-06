package distkvs

import (
	"container/heap"
	"container/list"
	"time"

	"example.org/cpsc416/a5/kvslib"
	concurmap "github.com/orcaman/concurrent-map"
)

func (f *FrontEnd) EstablishResourceForClient(clientId *string, reply *kvslib.KVSResponseInitialize) error {
	prioQueue := make(PriorityQueue, 0)
	f.clientMap.SetIfAbsent(*clientId, &prioQueue)
	f.opIdMap.SetIfAbsent(*clientId, uint32(0))

	reply.Err = false
	return nil
}

func (f *FrontEnd) HandleReadRequest(clientGetRequest *kvslib.KvslibGet, reply *kvslib.KvslibGetResult) error {
	//newGetChannel receives the response and sends it back to client
	newGetChannel := make(chan FrontEndResult, 1)
	requestItem := &Item{ClientId: clientGetRequest.ClientId, OpId: clientGetRequest.OpId, Key: clientGetRequest.Key, Request: "get", ResponseChannel: newGetChannel}
	//get the priority queue
	pq, _ := f.clientMap.Get(clientGetRequest.ClientId)
	heap.Push(pq.(*PriorityQueue), requestItem)

	ok := f.threadMap.SetIfAbsent(clientGetRequest.ClientId, true)
	if ok {
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
	//newPutChannel receives the response and sends it back to client
	newPutChannel := make(chan FrontEndResult, 1)
	requestItem := &Item{ClientId: clientPutRequest.ClientId, OpId: clientPutRequest.OpId, Key: clientPutRequest.Key, Value: clientPutRequest.Value, Request: "put", ResponseChannel: newPutChannel}
	//get the priority queue
	pq, _ := f.clientMap.Get(clientPutRequest.ClientId)
	heap.Push(pq.(*PriorityQueue), requestItem)
	ok := f.threadMap.SetIfAbsent(clientPutRequest.ClientId, true)
	if ok {
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
		firstItem := first.(*PriorityQueue).Peek().(*Item)
		//checks if the front element's opid in pq is the same as what frontend knows
		if clientOpId, _ := f.opIdMap.Get(clientId); firstItem.OpId == clientOpId.(uint32) {
			//pop front element and push it to keyMap's queue
			item := heap.Pop(first.(*PriorityQueue)).(*Item)
			frontEndRequest := FrontEndRequest{RequestType: item.Request, Key: item.Key, Value: item.Value, ClientId: item.ClientId, opID: item.OpId, ResponseChannel: item.ResponseChannel}
			f.keyMap.SetIfAbsent(item.Key, list.New())
			queue, _ := f.keyMap.Get(item.Key)
			queue.(*list.List).PushBack(frontEndRequest)
			f.opIdMap.Set(clientId, clientOpId.(uint32)+1)

			//create a goroutine to handle sending request to storage if it doesn't exist
			ok := f.threadMap.SetIfAbsent(item.Key, true)
			if ok {
				go f.sendRequestToStorage(item.Key)
			}
		} else {
			break
		}
	}
}

func (f *FrontEnd) sendRequestToStorage(key string) {
	defer f.threadMap.Remove(key)
	for keyQueue, _ := f.keyMap.Get(key); keyQueue.(*list.List).Len() > 0; {
		frontEle := keyQueue.(*list.List).Front()
		keyQueue.(*list.List).Remove(frontEle)
		request := FrontEndRequest(frontEle.Value.(FrontEndRequest))
		if request.RequestType == "get" {
			//get request sends request to a storage node. Use round robin
			for {
				f.queueMu.Lock()
				if f.storageQueue.Len() == 0 {
					f.queueMu.Unlock()
					request.ResponseChannel <- FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: request.Key, Value: "System Failed", RequestType: request.RequestType, Err: true}
					break
				} else {
					frontStorage := f.storageQueue.Front()
					f.storageQueue.Remove(frontStorage)
					if v, _ := f.storageConnCMap.Get(frontStorage.Value.(*StorageConn).StorageId); !v.(bool) {
						f.queueMu.Unlock()
						continue
					}
					f.storageQueue.PushBack(frontStorage.Value.(*StorageConn))
					f.queueMu.Unlock()
					getChan := make(chan FrontEndResult, 1)
					f.sendGetRequest(request, frontStorage.Value.(*StorageConn), getChan)
					returnedGetRequest := <-getChan
					if !returnedGetRequest.Err {
						request.ResponseChannel <- returnedGetRequest
						break
					}
				}
			}
		} else {
			//put request fans out to all joined and joining storage nodes
			putChan := make(chan FrontEndResult, 256)
			storageNodeNumber := 0
			for sNode := range f.storageConnMap.IterBuffered() {
				storageNodeNumber++
				go f.sendPutRequest(request, sNode, putChan)
			}

			//if there are no storage nodes to send request out to
			if storageNodeNumber == 0 {
				close(putChan)
				request.ResponseChannel <- FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: request.Key, Value: request.Value, RequestType: request.RequestType, Err: true}
				continue
			}

			for storageNodeNumber > 0 {
				returnedPutRequest := <-putChan
				storageNodeNumber--
				if !returnedPutRequest.Err {
					//take the first valid response and move on to the next request
					request.ResponseChannel <- returnedPutRequest
					break
				}
				if storageNodeNumber == 0 {
					request.ResponseChannel <- returnedPutRequest
				}
			}
		}
	}
}

func (f *FrontEnd) sendGetRequest(request FrontEndRequest, storeConn *StorageConn, getChan chan FrontEndResult) {
	getRequest := FrontEndGet{Key: request.Key}
	var getResult FrontEndResult

	var reply StorageGetResult
	gettingRequest := storeConn.client.Go("Storage.GetStorage", getRequest, &reply, nil)
	select {
	case res := <-gettingRequest.Done:
		resReply := res.Reply.(*StorageGetResult)
		getResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: resReply.Key, Value: "Node Failed", RequestType: request.RequestType}
		if res.Error != nil {
			getResult.Err = true
			f.queueMu.Lock()
			f.RemoveStorageNode(storeConn.StorageId, storeConn.client)
			f.storageConnCMap.Set(storeConn.StorageId, false)
			f.queueMu.Unlock()
		} else {
			getResult.Err = false
			getResult.Value = *resReply.Value
		}
	case <-time.After(time.Duration(timeOutPeriod) * time.Second):
		close(gettingRequest.Done)
		temp := storeConn.client.Go("Storage.GetStorage", getRequest, &reply, nil)
		select {
		case res := <-temp.Done:
			resReply := res.Reply.(*StorageGetResult)
			getResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: resReply.Key, Value: "Node Failed", RequestType: request.RequestType}
			if res.Error != nil {
				getResult.Err = true
				f.queueMu.Lock()
				f.RemoveStorageNode(storeConn.StorageId, storeConn.client)
				f.storageConnCMap.Set(storeConn.StorageId, false)
				f.queueMu.Unlock()
			} else {
				getResult.Err = false
				getResult.Value = *resReply.Value
			}
		case <-time.After(time.Duration(timeOutPeriod) * time.Second):
			close(temp.Done)
			getResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: request.Key, Value: "Node Failed", RequestType: request.RequestType, Err: true}
			f.queueMu.Lock()
			f.RemoveStorageNode(storeConn.StorageId, storeConn.client)
			f.storageConnCMap.Set(storeConn.StorageId, false)
			f.queueMu.Unlock()
		}
	}
	getChan <- getResult
}

func (f *FrontEnd) sendPutRequest(request FrontEndRequest, store concurmap.Tuple, putChan chan FrontEndResult) {
	putRequest := FrontEndPut{Key: request.Key, Value: request.Value}
	var putResult FrontEndResult
	if storeCon, ok := f.storageConnMap.Get(store.Key); ok {
		var reply StorageGetResult
		puttingRequest := storeCon.(*StorageConn).client.Go("Storage.PutStorage", putRequest, &reply, nil)
		select {
		case res := <-puttingRequest.Done:
			resReply := res.Reply.(*StorageGetResult)
			putResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: resReply.Key, Value: *resReply.Value, RequestType: request.RequestType}
			if !resReply.Response {
				putResult.Err = true
			}
			if res.Error != nil {
				putResult.Err = true
				f.RemoveStorageNode(store.Key, storeCon.(*StorageConn).client)
			} else {
				putResult.Err = false
			}
		case <-time.After(time.Duration(timeOutPeriod) * time.Second):
			close(puttingRequest.Done)
			temp := storeCon.(*StorageConn).client.Go("Storage.PutStorage", putRequest, &reply, nil)
			select {
			case res := <-temp.Done:
				resReply := res.Reply.(*StorageGetResult)
				putResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: resReply.Key, Value: *resReply.Value, RequestType: request.RequestType}
				if !resReply.Response {
					putResult.Err = true
				}
				if res.Error != nil {
					putResult.Err = true
					f.RemoveStorageNode(store.Key, storeCon.(*StorageConn).client)
				} else {
					putResult.Err = false
				}
			case <-time.After(time.Duration(timeOutPeriod) * time.Second):
				close(temp.Done)
				putResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: request.Key, RequestType: request.RequestType, Err: true}
				f.RemoveStorageNode(store.Key, storeCon.(*StorageConn).client)
			}
		}
	} else {
		putResult = FrontEndResult{ClientId: request.ClientId, opID: request.opID, Key: request.Key, RequestType: request.RequestType, Err: true}
	}
	putChan <- putResult
}
