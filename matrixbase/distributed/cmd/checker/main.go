package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

func main() {
	t := time.NewTimer(100*time.Second)
	//t := time.NewTimer(5*time.Minute)
	i := 0
Loop:
	for {
		select {
		case <- t.C:
			break Loop
		default:
			i++
			for {
				WriteData()
			}
		}
	}
}

func WriteData(){
	wg := &sync.WaitGroup{}

	for i := 1; i <= 1000; i++ {
		wg.Add(1)
		go func(i int, group *sync.WaitGroup) {
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
				fmt.Println(err)
				return
			}
			req.Header.Add("Content-Type", "application/json")

			res, err := client.Do(req)
			if err != nil {
				fmt.Println(err)
				return
			}
			defer res.Body.Close()
			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println("goroutine:" + fmt.Sprintf("%d",i) + string(body))
			group.Done()
		}(i,wg)
	}
	wg.Wait()
}