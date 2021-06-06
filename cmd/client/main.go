package main

import (
	"flag"
	"fmt"
	"log"
	"sync"

	distkvs "example.org/cpsc416/a5"
	kvslib "example.org/cpsc416/a5/kvslib"
)

var waitGroup sync.WaitGroup

func main() {
	var config distkvs.ClientConfig
	err := distkvs.ReadJSONConfig("../../config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client := distkvs.NewClient(config, *kvslib.NewKVS())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	for j := 0; j < 10; j++ {
		waitGroup.Add(1)
		go func(ew int) {
			defer waitGroup.Done()
			if _, err := client.Get("client1", fmt.Sprint(ew)); err != nil {
				log.Println(err)
			}
		}(j)

		if j == 5 {
			for jk := 0; jk < 5; jk++ {
				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					if _, err := client.Get("client1", "5"); err != nil {
						log.Println(err)
					}
				}()
			}
		}
	}

	for i := 0; i < 16; i++ {
		result := <-client.NotifyChannel
		log.Println(result)
	}
	waitGroup.Wait()
}
