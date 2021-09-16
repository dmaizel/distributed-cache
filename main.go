package main

import (
	"flag"
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

//func main() {
//cache.NewGroup("scores", 2<<10, cache.GetterFunc(
//func(key string) ([]byte, error) {
//log.Println("[SlowDB] search key", key)
//if v, ok := db[key]; ok {
//return []byte(v), nil
//}
//return nil, fmt.Errorf("%s does not exist", key)
//}))

//addr := "localhost:9999"
//peers := cache.NewHTTPPool(addr)
//log.Println("distache is running at", addr)
//log.Fatal(http.ListenAndServe(addr, peers))
//}

func main() {
	var port int
	var api bool

	flag.IntVar(&port, "port", 8001, "Distcache server port")
	flag.BoolVar(&api, "api", false, "Start an api server?")
	flag.Parse()

	apiAddr := "http://localhost:9999"
	addrMap := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}

	var addrs []string
	for _, v := range addrMap {
		addrs = append(addrs, v)
	}

	group := createGroup()
	if api {
		go startAPIServer(apiAddr, group)
	}

	startCacheServer(addrMap[port], []string(addrs), group)
}

func createGroup() *cache.Group {
	return cache.NewGroup("scores", 2<<10, cache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s does not exist", key)
		}))
}

func startAPIServer(apiAddr string, group *cache.Group) {
	http.Handle("/api", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			view, err := group.Get(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(view.ByteSlice())
		}))

	log.Println("fontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))
}

func startCacheServer(addr string, addrs []string, group *cache.Group) {
	peers := cache.NewHTTPPool(addr)
	peers.Set(addrs...)
	group.RegisterPeers(peers)
	log.Println("distcache is running at", addr)
	log.Fatal(http.ListenAndServe(addr[7:], peers))
}
