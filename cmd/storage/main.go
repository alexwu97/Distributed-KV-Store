package main

import (
	"log"

	distkvs "example.org/cpsc416/a5"
	"github.com/DistributedClocks/tracing"
	concurmap "github.com/orcaman/concurrent-map"
)

func main() {
	var config distkvs.StorageConfig
	err := distkvs.ReadJSONConfig("../../config/storage_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(config)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: "storage",
		Secret:         config.TracerSecret,
	})

	storage := distkvs.Storage{KVStore: concurmap.New()}
	err = storage.Start(config.FrontEndAddr, string(config.StorageAdd), config.DiskPath, tracer)
	if err != nil {
		log.Fatal(err)
	}
}
