package main

import (
    "encoding/json"
    "log"
    "fmt"
    "os"
    "net/http"
	"github.com/nats-io/nats.go"
	"os/signal"
    "time"
	"syscall"
)

type Info struct {
    Message string `json:"message"`
    Action string `json:"action"`
    Pause int `json:"pause"`
}

var nc *nats.Conn

func main() {
    http.HandleFunc("/process", process)
    log.Println("Server listen in 8080")

    nc, err := nats.Connect("0.0.0.0:4222")
    defer nc.Close()
    if err != nil {
        log.Println(err)
    }

    go createSuscribers(5, nc)
    log.Fatal(http.ListenAndServe(":8080", nil))


}

func process(w http.ResponseWriter, r *http.Request) {
    var info Info
    if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    jsonData, _ := json.Marshal(info)

    nc, err := nats.Connect("0.0.0.0:4222")
    defer nc.Close()
    if err != nil {
        log.Println(err)
    }
    nc.Publish("process", []byte(jsonData))

    respuesta := map[string]string{"response": "Info ok"}
    json.NewEncoder(w).Encode(respuesta)
}


func createSuscribers(quantity int, nc *nats.Conn) {
	for i := 1; i <= quantity ; i++ {
		subscriberID := i
		go Worker(subscriberID, nc)
	}
}

func Worker( workerId int, nc *nats.Conn){
    var info Info

    sub, err := nc.QueueSubscribe("process", "need_queue_group", func(msg *nats.Msg) {
	    err := json.Unmarshal([]byte(msg.Data), &info)
	    if err != nil {
	    	log.Fatal(err)
	    }
	    fmt.Printf("Worker %d received message - INIT : %s\n", workerId, info.Action)
	    fmt.Printf("%d RUN  FOR %d \n", workerId, info.Pause)
        time.Sleep(time.Duration(info.Pause)* time.Second)
	    fmt.Printf("Worker %d received message - END : %s\n", workerId, info.Action)
    })

	if err != nil {
		log.Printf("Subscriber %d failed to subscribe: %v", workerId, err)
		return
	}

	fmt.Printf("Subscriber %d is ready\n", workerId)
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	<-signalChannel
	sub.Unsubscribe()
}
