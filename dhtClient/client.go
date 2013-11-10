package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type KVData struct {
	Command string      `json:"Command"`
	Origin  string      `json:"Origin"`
	Key     uint32      `json:"Key"`
	Value   interface{} `json:"Value"`
}

type Message struct {
	Datatype string      `json:"DataType"`
	Data     interface{} `json:"Data"`
}

const (
	RECV_BUF_LEN = 1024
	PORT         = "8000"
)

func main() {
	if len(os.Args) < 5 {
		fmt.Println("Not enough arguments")
		return
	}

	our_ip := os.Args[1]
	contact_point := os.Args[2]
	command := os.Args[3]
	key, _ := strconv.Atoi(os.Args[4])
	value := "nil"

	if strings.ToUpper(command) == "LOOKUP" {
	} else if strings.ToUpper(command) == "DELETE" {
	} else if strings.ToUpper(command) == "INSERT" {
		if len(os.Args) < 6 {
			fmt.Println("Not enough arguments")
			return
		}
		value = os.Args[5]
	} else if strings.ToUpper(command) == "UPDATE" {
		if len(os.Args) < 6 {
			fmt.Println("Not enough arguments")
			return
		}
		value = os.Args[5]
	} else {
		fmt.Println("Incorrect command type. Aborting!!!")
		return
	}

	kvData := KVData{command, our_ip, uint32(key), value}

	// Send message to contact point
	m := createMessage("keyvalue", kvData)
	b, err := json.Marshal(m)

	recipientAddr, err := net.ResolveUDPAddr("udp", contact_point+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}

	if strings.ToUpper(command) == "LOOKUP" {
		waitForResponse()
	}
}

func waitForResponse() {
	// Wait for response
	sock := netSetup()
	var receivedMessage Message
	buf := make([]byte, RECV_BUF_LEN)
	rlen, _, err := sock.ReadFromUDP(buf)
	logError(err)
	err = json.Unmarshal(buf[:rlen], &receivedMessage)

	if receivedMessage.Datatype == "string" {
		fmt.Println(receivedMessage.Data.(string))
	} else {
		fmt.Println("Incorrect datatype received. Abort!")
	}
}

func createMessage(Datatype string, Data interface{}) Message {
	var retMessage Message
	retMessage.Datatype = Datatype
	retMessage.Data = Data
	return retMessage
}

func logError(err error) bool {
	if err != nil {
		fmt.Print("ERROR:")
		fmt.Print(err)
		fmt.Print(" ")
		fmt.Println(time.Now())
		return true
	}
	return false
}

func netSetup() *net.UDPConn {
	//Setup address
	addr, err := net.ResolveUDPAddr("udp", ":"+PORT)
	logError(err)

	//Setup socket
	sock, err := net.ListenUDP("udp", addr)
	logError(err)

	return sock
}
