package main

import (
	"fmt"
	"github.com/Jeffail/tunny"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
)

func main() {
	shutdown := make(chan struct{})
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		for sign := range signals {
			if sign == os.Interrupt || sign == syscall.SIGTERM {
				close(shutdown)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	numCPUs := runtime.NumCPU()
	pool := tunny.NewFunc(numCPUs, func(i interface{}) interface{} {
		url := "http://127.0.0.1:8081/kv"
		method := "POST"
		k := "dsfeqf" + fmt.Sprintf("%d",i)
		v := "ytuytyr" + fmt.Sprintf("%d",i)

		payload := strings.NewReader(`{
						"key":"`+k+`",
						"value":"`+v+`"
					}`)

		client := &http.Client {
		}
		req, err := http.NewRequest(method, url, payload)
		if err != nil {
			fmt.Println("new req:",err)
		}
		req.Header.Add("Content-Type", "application/json")

		res, err := client.Do(req)
		if err != nil {
			fmt.Println("req err:", err)
		}
		defer res.Body.Close()
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("goroutine:" + fmt.Sprintf("%d",i) + string(body))

		return nil
	})
	defer pool.Close()


	go func(p *tunny.Pool) {
		defer wg.Done()
		writeData(shutdown,p)
	}(pool)
	wg.Wait()
	fmt.Println("done")
}

func writeData(shutdown chan struct{},p *tunny.Pool) {
	for {
		select {
		case <-shutdown:
			fmt.Println("stop work")
			return
		default:
			for i:=0; i < 1000; i++ {
				go p.Process(i)
			}
		}
	}
}