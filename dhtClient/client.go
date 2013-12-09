package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type KVData struct {
	Command string      `json:"Command"`
	Origin  string      `json:"Origin"`
	Key     string      `json:"Key"`
	Value   interface{} `json:"Value"`
	Version float64     `json:"Version"`
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
		fmt.Println("Correct format is:")
		fmt.Println("-----")
		fmt.Println("<self_ip> <contact_ip> <command> <key> <value=optional>")
		return
	}

	our_ip := os.Args[1]
	contact_point := os.Args[2]
	command := os.Args[3]
	key := os.Args[4]
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
	kvData := KVData{command, our_ip, key, value, 0}

	// Send message to contact point
	m := createMessage("first", kvData)
	b, err := json.Marshal(m)

	recipientAddr, err := net.ResolveUDPAddr("udp", contact_point+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}

	upperCommand := strings.ToUpper(command)
	if upperCommand == "LOOKUP" || upperCommand == "INSERT" || upperCommand == "DELETE" || upperCommand == "UPDATE" {
		// Initialize benchmark time
		t0 := time.Now()

		waitForResponse()

		t1 := time.Now()
		fmt.Print(upperCommand + " took: ")
		fmt.Println(t1.Sub(t0))
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

	if receivedMessage.Datatype == "kvresp" {
		kv := convertToKVData(receivedMessage.Data)
		if kv.Command == "lookup" {
			fmt.Println(kv.Value)
		}
		fmt.Println("OK!")
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

func convertToKVData(genericData interface{}) KVData {
	command := genericData.(map[string]interface{})["Command"].(string)
	origin := genericData.(map[string]interface{})["Origin"].(string)
	key := genericData.(map[string]interface{})["Key"].(string)
	value := genericData.(map[string]interface{})["Value"]
	version := genericData.(map[string]interface{})["Version"].(float64)

	return KVData{command, origin, key, value, version}
}
