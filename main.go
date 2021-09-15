package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/dmaizel/distributed-cache/cache"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func main() {
	cache.NewGroup("scores", 2<<10, cache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s does not exist", key)
		}))

	addr := "localhost:9999"
	peers := cache.NewHTTPPool(addr)
	log.Println("distache is running at", addr)
	log.Fatal(http.ListenAndServe(addr, peers))
}
