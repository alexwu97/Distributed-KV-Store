package distkvs

import (
	"errors"
	"fmt"
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

type StorageLoadSuccess struct {
	State map[string]string
}

type StoragePut struct {
	Key   string
	Value string
}

type StorageSaveData struct {
	Key   string
	Value string
}

type StorageGet struct {
	Key string
}

type StorageGetResult struct {
	Key   string
	Value *string
}

type Storage struct {
	// state may go here
	KVStore concurmap.ConcurrentMap
	mu      sync.Mutex
}

var (
	fileName     string = "KVFile.txt"
	fullPathFile string
)

func (s *Storage) Start(frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
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
	//set up RPC handler and register the functions
	rpc.Register(s)

	listener, rpcErr := net.Listen("tcp", "localhost"+frontEndAddr)
	if rpcErr != nil {
		log.Fatal(rpcErr)
	}
	defer listener.Close()

	rpc.Accept(listener)

	return nil
}

func NewStorage() *Storage {
	return &Storage{KVStore: concurmap.New()}
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

func (t *Storage) GetStorage(args *FrontEndGet, result *StorageGetResult) error {
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

	writeKVToFile(fullPathFile, t, args)

	//update in store
	t.KVStore.Set(args.Key, args.Value)
	result.Key = args.Key
	result.Value = &args.Value
	fmt.Println(result.Value)
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
