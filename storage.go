package distkvs

import (
	"container/list"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"

	"github.com/DistributedClocks/tracing"
	concurmap "github.com/orcaman/concurrent-map"
)

type StorageConfig struct {
	StorageID        string
	StorageAdd       StorageAddr
	ListenAddr       string
	FrontEndAddr     string
	DiskPath         string
	TracerServerAddr string
	TracerSecret     []byte
}

type StorageGetResult struct {
	Key      string
	Value    *string
	Response bool
}

type StorageJoined struct {
	StorageID string
	State     map[string]string
}

type StorageStateResponse struct {
	StorageMap map[string]string
}

type Storage struct {
	// state may go here
	KVStore       concurmap.ConcurrentMap
	PutQueue      *list.List
	mu            *sync.Mutex
	storageClient *rpc.Client
	joinState     string
}

var (
	fileName     string = "KVFile.txt"
	fullPathFile string
	wg           sync.WaitGroup
)

func NewStorage() *Storage {
	return &Storage{KVStore: concurmap.New()}
}

func (s *Storage) Start(storageId string, frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
	s.mu = &sync.Mutex{}
	s.joinState = "unjoined"
	s.PutQueue = list.New()
	//check if file in disk exists, create new one if not
	fullPathFile = diskPath + fileName
	if _, err := os.Stat(fullPathFile); os.IsNotExist(err) {
		_, err = os.Create(fullPathFile)
		if err != nil {
			log.Fatal("cannot open")
		}
	}

	//update info in disk to storage KVstore
	s.UpdateFromFile(fullPathFile)

	//set up listener at storage port
	wg.Add(1)
	go s.listenOnPort(storageAddr)

	//connect to FrontEnd
	storageClient, err := ConnectToNode(frontEndAddr)
	if err != nil {
		log.Fatal(storageId + " cannot rpc connect to Frontend")
	}

	s.storageClient = storageClient

	storageJoinRequest := StorageJoinRequest{StorageId: storageId, StorageAddress: storageAddr}
	var requestJoinReply StorageJoinReply

	err = s.storageClient.Call("FrontEnd.RequestJoin", storageJoinRequest, &requestJoinReply)
	if err != nil {
		log.Fatal(storageId + " cannot connect to FrontEnd")
	}
	s.joinState = requestJoinReply.JoinState
	if s.joinState == "joining" {
		for sKey, sVal := range requestJoinReply.StorageMap {
			if sKey != storageId && sVal.JoinState == "joined" {
				clie, err := ConnectToNode(sVal.StorageAddress)
				if err == nil {
					var stateReply StorageStateResponse
					err = clie.Call("Storage.ProvideState", StorageJoinRequest{}, &stateReply)
					if err == nil {
						if s.updateState(stateReply.StorageMap) {
							s.joinState = "joined"
							storageUpdatedRequest := StorageUpToDateRequest{StorageId: storageId, StorageAddress: storageAddr, JoinState: s.joinState}
							var storageJoinedReply StorageUpToDateResponse
							err = s.storageClient.Call("FrontEnd.NotifyUpToDate", storageUpdatedRequest, &storageJoinedReply)
							if err != nil {
								log.Fatal(storageId + " cannot fully join frontend")
							}
							clie.Close()
							break
						}
					}
				}
				clie.Close()
			}
		}
	}
	storageClient.Close()
	wg.Wait()
	return nil
}

func (s *Storage) ProvideState(args *StorageJoinRequest, result *StorageStateResponse) error {
	result.StorageMap = s.generateMapState()
	return nil
}

func (s *Storage) UpdateFromFile(kvFile string) {

	mainFile, err := ioutil.ReadFile(kvFile)
	if err != nil {
		log.Fatal("failed opening kv file")
	}

	//get all the lines from the file
	lines := strings.Split(string(mainFile), "\n")
	linebreak := len(lines)
	//go through the lines, if it isn't correct, record the line index
	for i, line := range lines {
		tempArr := strings.Split(line, ":")
		if len(tempArr) != 2 {
			linebreak = i
			break
		}
		tempStr := tempArr[1]
		tempStrArr := []rune(tempStr)
		valueLen := len(tempStrArr)
		if valueLen < 1 || tempStrArr[valueLen-1] != '&' {
			linebreak = i
			break
		}
		tempArr[1] = tempArr[1][:len(tempArr[1])-1]
		s.KVStore.Set(tempArr[0], &tempArr[1])
	}

	//rewrite the file only up to the line index
	lines = lines[0:linebreak]
	lines[linebreak-1] = lines[linebreak-1] + "\n"
	output := strings.Join(lines, "\n")
	err = ioutil.WriteFile(kvFile, []byte(output), 0644)
	if err != nil {
		log.Fatal("could not rewrite entire file")
	}
}

func ConnectToNode(port string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", "localhost"+port)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (s *Storage) listenOnPort(storageAddr string) {
	defer wg.Done()

	//set up RPC handler and register the functions
	rpc.Register(s)
	listener, rpcErr := net.Listen("tcp", "localhost"+storageAddr)
	if rpcErr != nil {
		log.Fatal(rpcErr)
	}
	defer listener.Close()

	rpc.Accept(listener)
}

func (s *Storage) updateState(updatedState map[string]string) bool {
	for k, v := range updatedState {
		if err := s.updateFileAndStore(&FrontEndPut{Key: k, Value: v}); err != nil {
			return false
		}
	}

	s.handleQueueRequest()
	return true
}

func (t *Storage) GetStorage(args *FrontEndGet, result *StorageGetResult) error {
	for t.PutQueue.Len() > 0 {

	}
	result.Key = args.Key
	val, ok := t.KVStore.Get(args.Key)
	if !ok {
		return errors.New("no key exists")
	} else {
		result.Value = val.(*string)
	}
	return nil
}

func (t *Storage) PutStorage(args *FrontEndPut, result *StorageGetResult) error {
	if t.joinState == "joined" {
		if err := t.updateFileAndStore(args); err != nil {
			return err
		}
		result.Response = true
	} else {
		t.PutQueue.PushBack(args)
		result.Response = false
	}
	result.Key = args.Key
	result.Value = &args.Value
	return nil
}

func (t *Storage) updateFileAndStore(kvPair *FrontEndPut) error {
	if err := writeKVToFile(fullPathFile, t, kvPair); err != nil {
		return err
	}
	t.KVStore.Set(kvPair.Key, kvPair.Value)
	return nil
}

func writeKVToFile(KVDiskFile string, storage *Storage, args *FrontEndPut) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	file, err := os.OpenFile(KVDiskFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	newLine := args.Key + ":" + args.Value + "&\n"
	if _, err = file.WriteString(newLine); err != nil {
		log.Fatal("failed writing to file")
	}

	return nil
}

func (s *Storage) handleQueueRequest() {
	for s.PutQueue.Len() > 0 {
		frontReq := s.PutQueue.Front()
		s.PutQueue.Remove(frontReq)
		request := StorageGetResult(frontReq.Value.(StorageGetResult))
		if err := s.updateFileAndStore(&FrontEndPut{Key: request.Key, Value: *request.Value}); err != nil {
			log.Fatal("could not put info from storage queue into file/state")
		}
	}
}

func (s *Storage) generateMapState() map[string]string {
	newMap := make(map[string]string)
	for kv := range s.KVStore.IterBuffered() {
		newMap[kv.Key] = kv.Val.(string)
	}
	return newMap
}
