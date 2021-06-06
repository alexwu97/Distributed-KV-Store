package distkvs

import (
	"log"
	"net"
	"net/rpc"

	concurmap "github.com/orcaman/concurrent-map"
)

func (f *FrontEnd) StartListeningOnTcpPort(port string) {
	defer wgF.Done()
	listener, rpcErr := net.Listen("tcp", "localhost"+port)
	if rpcErr != nil {
		log.Fatal(rpcErr)
	}
	defer listener.Close()

	rpc.Accept(listener)
}

func (f *FrontEnd) RequestJoin(args *StorageJoinRequest, reply *StorageJoinReply) error {
	newStorageNode := StorageNode{StorageAddress: args.StorageAddress}
	if f.storageMap.IsEmpty() {
		newStorageNode.JoinState = "joined"
		reply.StorageMap = nil
	} else {
		newStorageNode.JoinState = "joining"
		reply.StorageMap = generateRegularMap(f.storageMap)
	}
	reply.JoinState = newStorageNode.JoinState

	f.storageMap.Set(args.StorageId, newStorageNode)
	client, err := ConnectToStorage(newStorageNode.StorageAddress)
	if err != nil {
		log.Fatal("cannot rpc connect to storage: " + args.StorageId)
	}
	storageCon := StorageConn{client: client, StorageId: args.StorageId}
	f.storageConnMap.Set(args.StorageId, &storageCon)
	f.queueMu.Lock()
	f.storageConnCMap.SetIfAbsent(args.StorageId, false)
	inQueue, _ := f.storageConnCMap.Get(args.StorageId)
	if newStorageNode.JoinState == "joined" && !inQueue.(bool) {
		f.storageConnCMap.Set(args.StorageId, true)
		f.storageQueue.PushBack(&storageCon)
	}
	f.queueMu.Unlock()
	return nil
}

func ConnectToStorage(port string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", "localhost"+port)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (f *FrontEnd) NotifyUpToDate(args *StorageUpToDateRequest, reply *StorageUpToDateResponse) error {
	newStorageNode := StorageNode{StorageAddress: args.StorageAddress, JoinState: args.JoinState}
	f.storageMap.Set(args.StorageId, newStorageNode)
	f.queueMu.Lock()
	f.storageConnCMap.SetIfAbsent(args.StorageId, false)
	inQueue, _ := f.storageConnCMap.Get(args.StorageId)
	if newStorageNode.JoinState == "joined" && !inQueue.(bool) {
		f.storageConnCMap.Set(args.StorageId, true)
		connection, _ := f.storageConnMap.Get(args.StorageId)
		f.storageQueue.PushBack(connection.(*StorageConn))
	}
	f.queueMu.Unlock()
	reply.success = true
	return nil
}

func (f *FrontEnd) RemoveStorageNode(key string, client *rpc.Client) {
	f.storageMap.Remove(key)
	client.Close()
	f.storageConnMap.Remove(key)
}

func generateRegularMap(storeageMap concurmap.ConcurrentMap) map[string]StorageNode {
	newMap := make(map[string]StorageNode)
	for node := range storeageMap.IterBuffered() {
		newMap[node.Key] = StorageNode{JoinState: node.Val.(StorageNode).JoinState, StorageAddress: node.Val.(StorageNode).StorageAddress}
	}
	return newMap
}
