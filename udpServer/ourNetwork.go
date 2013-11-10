package main

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Message struct {
	Datatype string      `json:"DataType"`
	Data     interface{} `json:"Data"`
}

func createMessage(Datatype string, Data interface{}) Message {
	var retMessage Message
	retMessage.Datatype = Datatype
	retMessage.Data = Data
	return retMessage
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

/*
 * sends heartbeats regularly
 * to machines in the system
 * @param conn the socket
 * @param message heartBeat, failure, timestamp
 */
func sendHeartBeat(members map[string]Entry, selfName string) {
	m := createMessage("gossip", members)
	b, err := json.Marshal(m)
	kMembers := pickAdresses(members, K, selfName)
	logError(err)
	for i := range kMembers {
		recipientId := kMembers[i]

		//split to timestamp and ip address
		a := strings.Split(recipientId, "#")
		//memberIp = ip
		recipientIp := a[1]
		//retrieve a UDPaddr
		recipientAddr, err := net.ResolveUDPAddr("udp", recipientIp+":"+PORT)
		logError(err)
		conn, err := net.DialUDP("udp", nil, recipientAddr)
		if !logError(err) {
			conn.Write(b)
			conn.Close()
		}
	}
}

func sendKV(targetIp string, data KVData) {
	m := createMessage("keyvalue", data)
	b, err := json.Marshal(m)

	recipientAddr, err := net.ResolveUDPAddr("udp", targetIp+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}
}

/*
 * receives heartBeats from other machines
 * updates timestamps
 * @param rlen length of the message received
 * @param remote address of the machine that sent the heartBeat
 * @param buf the byte array containing the messages
 */
func recvHeartBeat(sock *net.UDPConn, myMembers map[string]Entry, selfName string, myKeyValue KeyValue) {
	for {
		//we should change the byte length in the future
		//First initialize connection
		buf := make([]byte, RECV_BUF_LEN)
		rlen, _, err := sock.ReadFromUDP(buf)
		if QUIT == true {
			return
		}
		logError(err)

		//Second, setting up member information from retrieved value
		var receivedMessage Message
		err = json.Unmarshal(buf[:rlen], &receivedMessage)

		if receivedMessage.Datatype == "gossip" {
			receivedMessageData := convertToEntryMap(receivedMessage.Data)
			gossipProtocolHandler(receivedMessageData, myMembers)
			FIRST_GOSSIP_RECIEVED = true
		} else if receivedMessage.Datatype == "keyvalue" {
			receivedMessageData := convertToKVData(receivedMessage.Data)
			keyValueProtocolHandler(receivedMessageData, myMembers, selfName, myKeyValue)
		} else if receivedMessage.Datatype == "string" {
			fmt.Println(receivedMessage.Data.(string))
		} else if receivedMessage.Datatype == "requestkv" {
			originIp := receivedMessage.Data.(string)
			if originIp != strings.Split(selfName, "#")[1] {
				requestkvProtocolHandler(originIp, selfName, myKeyValue)
			}
		} else if receivedMessage.Datatype == "batchkeys" {
			fmt.Println(receivedMessage.Data)
			batchkeysProtocolHandler(receivedMessage.Data, myKeyValue)
		}
		if err != nil {
			fmt.Print("MARSHALFAIL:")
			fmt.Print(err)
			fmt.Println(time.Now())
		}
	}
}

//////
func batchkeysProtocolHandler(receivedMessageData interface{}, myKeyValue KeyValue) {
	fmt.Println("Going to update batch keys")
	for key, value := range receivedMessageData.(map[string]interface{}) {
		intKey, _ := strconv.Atoi(key)
		myKeyValue.data[uint32(intKey)] = value
	}
}

//////
func requestkvProtocolHandler(originIp string, selfName string, myKeyValue KeyValue) {
	hashedOriginIp := createHash(originIp)
	var SendKeyValue map[string]interface{}
	SendKeyValue = make(map[string]interface{})

	//iterate and populate sendKeyValue with appropriate keys
	for key, _ := range myKeyValue.data {
		if key < hashedOriginIp {
			SendKeyValue[strconv.Itoa(int(key))] = myKeyValue.data[key]
		}
	}

	//delete the keys that were added to sendKeyValue
	/*
		for key, _ := range SendKeyValue {
			delete(myKeyValue.data, key)
		}*/

	//send sendKeyValue over the network
	m := createMessage("batchkeys", SendKeyValue)

	b, err := json.Marshal(m)

	recipientAddr, err := net.ResolveUDPAddr("udp", originIp+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}

}

func gossipProtocolHandler(receivedMembers map[string]Entry, myMembers map[string]Entry) {
	/////

	//compare newList to mylist
	//	1) if higher hbc, update mylist with new hbc and new timestamp
	//	2) else, do nothing
	for receivedKey, _ := range receivedMembers {
		receivedValue := receivedMembers[receivedKey]

		// If our current membership list contains the received member
		if myValue, exists := myMembers[receivedKey]; exists {
			compareMembers(receivedKey, receivedValue, myValue, myMembers)
		} else {
			if receivedValue.Leave == false {
				var entry Entry
				entry.Failure = false
				entry.Hbc = receivedValue.Hbc
				entry.Timestamp = time.Now().Unix()
				entry.Leave = receivedValue.Leave
				myMembers[receivedKey] = entry

				//log joins
				fmt.Print("JOIN:")
				fmt.Print(receivedKey + " joined the system ")
				fmt.Println(time.Now())
			}
		}
	}
}

func keyValueProtocolHandler(receivedData KVData, myMembers map[string]Entry, selfName string, myKeyValue KeyValue) {
	key := createHash(string(receivedData.Key))
	selfIp := strings.Split(selfName, "#")[1]
	// If it should be handled locally, use kv.go
	targetName, _ := findSuccessor(key, selfName, myMembers)
	targetIp := strings.Split(targetName, "#")[1]

	if targetIp == selfIp {
		if receivedData.Command == "insert" {
			myKeyValue.Insert(string(receivedData.Key), receivedData.Value)
		} else if receivedData.Command == "lookup" {
			message := myKeyValue.Lookup(string(receivedData.Key))
			sendMessageToOrigin(receivedData.Origin, message)
		} else if receivedData.Command == "update" {
			myKeyValue.Update(string(receivedData.Key), receivedData.Value)
		} else if receivedData.Command == "delete" {
			myKeyValue.Delete(receivedData.Key)
		}
	} else {
		sendKV(targetIp, receivedData)
	}

}

func sendMessageToOrigin(targetIp string, message interface{}) {
	m := createMessage("string", message)
	b, err := json.Marshal(m)

	recipientAddr, err := net.ResolveUDPAddr("udp", targetIp+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}
}

func compareMembers(inputKey string, inputValue Entry, storedValue Entry, storedMembersList map[string]Entry) {
	if inputValue.Leave == true {
		if storedMembersList[inputKey].Leave == false {
			entry := storedMembersList[inputKey]
			entry.Leave = true
			entry.Timestamp = time.Now().Unix()
			storedMembersList[inputKey] = entry

			//log leaves
			fmt.Print("LEAVE:")
			fmt.Print(inputKey + " left the system ")
			fmt.Println(time.Now())
		}
	} else if inputValue.Hbc > storedValue.Hbc {
		if storedMembersList[inputKey].Leave == false {
			inputValue.Timestamp = time.Now().Unix()
			inputValue.Failure = false
			storedMembersList[inputKey] = inputValue
		}
	}
}

func notifyContactPoint(members map[string]Entry, selfName string) {
	m := createMessage("gossip", members)
	b, err := json.Marshal(m)
	//send to contact point
	memberAddr, err := net.ResolveUDPAddr("udp", CONTACT_POINT+":"+PORT)
	logError(err)
	//
	conn, err := net.DialUDP("udp", nil, memberAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
		//log join
		fmt.Print("JOIN:")
		fmt.Print(selfName + " joined the system ")
		fmt.Println(time.Now())
	}
}

func convertToEntryMap(genericData interface{}) map[string]Entry {
	var members map[string]Entry
	members = make(map[string]Entry)

	for key, _ := range genericData.(map[string]interface{}) {
		result := genericData.(map[string]interface{})[key]

		newHbc := result.(map[string]interface{})["Hbc"].(float64)
		newTimestamp := result.(map[string]interface{})["Timestamp"].(float64)
		newFailure := result.(map[string]interface{})["Failure"].(bool)
		newLeave := result.(map[string]interface{})["Leave"].(bool)

		members[key] = Entry{int64(newHbc), int64(newTimestamp), newFailure, newLeave}
	}

	return members
}

func convertToKVData(genericData interface{}) KVData {
	command := genericData.(map[string]interface{})["Command"].(string)
	origin := genericData.(map[string]interface{})["Origin"].(string)
	key := genericData.(map[string]interface{})["Key"].(float64)
	value := genericData.(map[string]interface{})["Value"]

	return KVData{command, origin, uint32(key), value}
}
