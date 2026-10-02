package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	num        = flag.Int("num", 10000, "num channels")
	tcpAddress = flag.String("nsqd-tcp-address", "127.0.0.1:4150", "<addr>:<port> to connect to nsqd")
)

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *num; j++ {
		wg.Add(1)
		go func(id int) {
			subWorker(*num, *tcpAddress, fmt.Sprintf("t%d", j), "ch", rdyChan, goChan, id)
			wg.Done()
		}(j)
		<-rdyChan
		time.Sleep(5 * time.Millisecond)
	}

	close(goChan)
	wg.Wait()
}

func subWorker(n int, tcpAddr string,
	topic string, channel string,
	rdyChan chan int, goChan chan int, id int) {
	conn, err := net.DialTimeout("tcp", tcpAddr, time.Second)
	if err != nil {
		panic(err.Error())
	}
	_, err = conn.Write(nsq.MagicV2)
	if err != nil {
		panic(err.Error())
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	ci := make(map[string]interface{})
	ci["client_id"] = "test"
	cmd, _ := nsq.Identify(ci)
	_, err = cmd.WriteTo(rw)
	if err != nil {
		panic(err.Error())
	}
	_, err = nsq.Subscribe(topic, channel).WriteTo(rw)
	if err != nil {
		panic(err.Error())
	}
	rdyCount := 1
	rdy := rdyCount
	rdyChan <- 1
	<-goChan
	_, err = nsq.Ready(rdyCount).WriteTo(rw)
	if err != nil {
		panic(err.Error())
	}
	err = rw.Flush()
	if err != nil {
		panic(err.Error())
	}
	_, err = nsq.ReadResponse(rw)
	if err != nil {
		panic(err.Error())
	}
	_, err = nsq.ReadResponse(rw)
	if err != nil {
		panic(err.Error())
	}
	for {
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			panic(err.Error())
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			panic(err.Error())
		}
		if frameType == nsq.FrameTypeError {
			panic(string(data))
		} else if frameType == nsq.FrameTypeResponse {
			_, err = nsq.Nop().WriteTo(rw)
			if err != nil {
				panic(err.Error())
			}
			err = rw.Flush()
			if err != nil {
				panic(err.Error())
			}
			continue
		}
		msg, err := nsq.DecodeMessage(data)
		if err != nil {
			panic(err.Error())
		}
		_, err = nsq.Finish(msg.ID).WriteTo(rw)
		if err != nil {
			panic(err.Error())
		}
		rdy--
		if rdy == 0 {
			_, err = nsq.Ready(rdyCount).WriteTo(rw)
			if err != nil {
				panic(err.Error())
			}
			rdy = rdyCount
			err = rw.Flush()
			if err != nil {
				panic(err.Error())
			}
		}
	}
}
