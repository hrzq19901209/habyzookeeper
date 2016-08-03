package main

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"time"
)

var (
	currentIp string
)

func must(other string, err error) {
	if err != nil {
		panic(fmt.Sprintf("%s:   %s", other, err))
	}
}

func connect(server string) *zk.Conn {
	zks := strings.Split(server, ",")
	conn, _, err := zk.Connect(zks, time.Second)
	must("connect", err)
	return conn
}

func mirror(conn *zk.Conn, path string) (chan []string, chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)
	go func() {
		for {
			snapshot, _, events, err := conn.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}
			snapshots <- snapshot //必须在evt := <-events
			evt := <-events
			fmt.Println("changing....")
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()
	return snapshots, errors
}

func GetLeader(conn *zk.Conn, masters []string, path string) {
	if len(masters) == 0 {
		fmt.Println("No leader")
		fmt.Println("I have to tell dns no leader")
		currentIp = ""
		return
	}
	leader := masters[0]
	for _, name := range masters {
		ret := strings.Compare(name, leader)
		if ret < 0 {
			leader = name
		}
	}

	fmt.Println(leader)
	leaderIp, _, err := conn.Get(fmt.Sprintf("%s/%s", path, leader))
	must("GetLeader", err)
	fmt.Printf("leaderIp: %s\n", leaderIp)
	if strings.Compare(string(leaderIp), currentIp) == 0 {
		fmt.Println("Oh, The leader does not change, I dont need to tell the dns")
	} else {
		fmt.Println("Oh, The leader change, I have to tell the dns")
		currentIp = string(leaderIp)
	}
}

func init() {
	currentIp = ""
}

func main() {
	conn := connect("127.0.0.1:2181")
	defer conn.Close()

	snapshots, errors := mirror(conn, "/zk_test")
	for {
		select {
		case snapshot := <-snapshots:
			fmt.Printf("%+v\n", snapshot)
			GetLeader(conn, snapshot, "/zk_test")
		case err := <-errors:
			panic(err)
		}
	}
}
