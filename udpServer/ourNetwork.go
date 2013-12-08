package main

import (
	"encoding/json"
	"fmt"
	"net"
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
	m := createMessage("first", data)
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
		} else if receivedMessage.Datatype == "keyvalue" {
			receivedMessageData := convertToKVData(receivedMessage.Data)
			keyValueProtocolHandler(receivedMessageData, myMembers, selfName, myKeyValue)
		} else if receivedMessage.Datatype == "string" {
			fmt.Println(receivedMessage.Data.(string))
		} else if receivedMessage.Datatype == "batchkeys" {
			batchkeysProtocolHandler(receivedMessage.Data, myKeyValue)
		} else if receivedMessage.Datatype == "updateRM" {
			receivedMessageData := convertToRM(receivedMessage.Data)
			updateRMProtocolHandler(receivedMessageData, myMembers)
		} else if receivedMessage.Datatype == "elected" {
			receivedMessageData := convertToKVData(receivedMessage.Data)
			leaderProtocolHandler(receivedMessageData, myMembers)
		} else if receivedMessage.Datatype == "first" {
			receivedMessageData := convertToKVData(receivedMessage.Data)
			firstKeyValueCommandHandler(receivedMessageData, myMembers)
		} else if receivedMessage.Datatype == "leader-ask" {
			requesting_ip := receivedMessage.Data.(string)
			leaderTellHandler(requesting_ip)
		} else if receivedMessage.Datatype == "leader-tell" {
			RM_LEADER = receivedMessage.Data.(string)
		} else if receivedMessage.Datatype == "rmRequest" {
			requesting_ip := receivedMessage.Data.(string)
			rmRequestHandler(requesting_ip)
		} else if receivedMessage.Datatype == "askforvalue" {
			requestValueHandler(receivedMessage.Data.(string), myKeyValue)
		} else if receivedMessage.Datatype == "fillSparseEntry" {
			fillSparseEntryHandler(receivedMessage.Data.(string), myMembers)
		}
		if err != nil {
			fmt.Print("MARSHALFAIL:")
			fmt.Print(err)
			fmt.Println(time.Now())
		}
	}
}

func crashHandler(crashed_ip string, myMembers map[string]Entry) {
	rmData := RM.GetEntireRmData()

	for key, ipAddrs := range rmData {
		//Check if crashed ip is in ip adress list
		containsCrashedIp := false
		goodIps := make([]string, 0)
		for index, _ := range ipAddrs {
			if ipAddrs[index] == crashed_ip {
				containsCrashedIp = true
			} else {
				goodIps = append(goodIps, ipAddrs[index])
			}
		}

		if containsCrashedIp {
			newIp := "baby"
			results := pickAdressesFilterThese(myMembers, 1, goodIps)
			RM.Replace(key, goodIps)

			if results != nil {
				if !myMembers[results[0]].Failure {
					newIp = strings.Split(results[0], "#")[1]
					fillSparseEntryHandler(newIp, myMembers)
				}
			} else {
				fmt.Println("WARNING: No replacement RMs found")
			}
		}
	}
}

func requestValueHandler(receivedMessage string, myKeyValue KeyValue) {
	targetIp := strings.Split(receivedMessage, "#")[0]
	key := strings.Split(receivedMessage, "#")[1]
	kvData := KVData{"insert", SELF_IP, key, myKeyValue.Lookup(key)}
	value := createMessage("keyvalue", kvData)
	b, _ := json.Marshal(value)

	recipientAddr, err := net.ResolveUDPAddr("udp", targetIp+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}
}

func fillSparseEntryHandler(recipient_IP string, myMembers map[string]Entry) {

	//update RM that has sparse entries
	allRms := RM.GetEntireRmData()

	for key, value := range allRms {
		if len(value) < REPLICA_LEVEL {
			ips := RM.GetAll(key)
			for idx, _ := range ips {
				//send request here with ip address and key
				kvMsg := createMessage("askforvalue", recipient_IP+"#"+key)
				b, _ := json.Marshal(kvMsg)
				targetIp := ips[idx]
				recipientAddr, err := net.ResolveUDPAddr("udp", targetIp+":"+PORT)
				logError(err)
				conn, err := net.DialUDP("udp", nil, recipientAddr)
				if !logError(err) {
					conn.Write(b)
					conn.Close()
				}
			}

			// Update RM
			RM.Insert(key, recipient_IP)
		}
	}
	//broadcast rm
	broadcastRM := createMessage("updateRM", RM.GetEntireRmData())
	b, _ := json.Marshal(broadcastRM)
	for key, _ := range myMembers {
		targetIp := strings.Split(key, "#")[1]
		recipientAddr, err := net.ResolveUDPAddr("udp", targetIp+":"+PORT)
		logError(err)
		conn, err := net.DialUDP("udp", nil, recipientAddr)
		if !logError(err) {
			conn.Write(b)
			conn.Close()
		}
	}
}

func rmRequestHandler(requestor_ip string) {
	sendData := make(map[string][]string)
	sendData = RM.GetEntireRmData()

	mappingMsg := createMessage("updateRM", sendData)
	b, _ := json.Marshal(mappingMsg)

	recipientAddr, err := net.ResolveUDPAddr("udp", requestor_ip+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}
}

func leaderAskHandler(contact_ip string, self_ip string) {
	msg := createMessage("leader-ask", self_ip)
	b, _ := json.Marshal(msg)

	// send msg to leader
	recipientAddr, err := net.ResolveUDPAddr("udp", contact_ip+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}
}

func leaderTellHandler(requestor_ip string) {
	msg := createMessage("leader-tell", RM_LEADER)
	b, _ := json.Marshal(msg)

	// send msg to leader
	recipientAddr, err := net.ResolveUDPAddr("udp", requestor_ip+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}
}

func firstKeyValueCommandHandler(receivedData KVData, myMembers map[string]Entry) {
	if RM.Exists(receivedData.Key) {
		ips := RM.GetAll(receivedData.Key)
		kvMsg := createMessage("keyvalue", receivedData)
		b, _ := json.Marshal(kvMsg)
		for key, _ := range ips {
			targetIp := ips[key]
			recipientAddr, err := net.ResolveUDPAddr("udp", targetIp+":"+PORT)
			logError(err)
			conn, err := net.DialUDP("udp", nil, recipientAddr)
			if !logError(err) {
				conn.Write(b)
				conn.Close()
			}
		}
	} else {
		if RM_LEADER != "empty" {
			msg := createMessage("elected", receivedData)
			b, _ := json.Marshal(msg)

			// send msg to leader
			recipientAddr, err := net.ResolveUDPAddr("udp", RM_LEADER+":"+PORT)
			logError(err)
			conn, err := net.DialUDP("udp", nil, recipientAddr)
			if !logError(err) {
				conn.Write(b)
				conn.Close()
			}
		} else {
			electionProtocolHandler(receivedData, myMembers)
		}
	}
}

// elect leader and send message
func electionProtocolHandler(receivedData KVData, myMembers map[string]Entry) {
	leaderSuffix := "1"
	leader := ""

	for key, _ := range myMembers {
		ip := strings.Split(key, "#")[1]
		suffix := strings.Split(ip, ".")[3]
		if suffix > leaderSuffix {
			leader = ip
		}
	}

	RM_LEADER = leader

	// create message to send to new leader
	msg := createMessage("elected", receivedData)
	b, _ := json.Marshal(msg)

	// send msg to leader
	recipientAddr, err := net.ResolveUDPAddr("udp", leader+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}
}

// you are elected as the leader
func leaderProtocolHandler(receivedData KVData, myMembers map[string]Entry) {

	if !RM.Exists(receivedData.Key) {
		//pick REPLICA_LEVEL ips
		ips := pickAdresses(myMembers, REPLICA_LEVEL, "does not matter")
		//leader creates mapping
		for key, _ := range ips {
			ip := strings.Split(ips[key], "#")[1]
			RM.Insert(receivedData.Key, ip)
		}
	}
	//leader updates everyone with mapping
	sendData := make(map[string][]string)
	sendData[receivedData.Key] = RM.GetAll(receivedData.Key)
	mappingMsg := createMessage("updateRM", sendData)
	b, _ := json.Marshal(mappingMsg)
	for key, _ := range myMembers {
		targetIp := strings.Split(key, "#")[1]
		recipientAddr, err := net.ResolveUDPAddr("udp", targetIp+":"+PORT)
		logError(err)
		conn, err := net.DialUDP("udp", nil, recipientAddr)
		if !logError(err) {
			conn.Write(b)
			conn.Close()
		}
	}
	//send kv messages
	ips := RM.GetAll(receivedData.Key)
	kvMsg := createMessage("keyvalue", receivedData)
	b, _ = json.Marshal(kvMsg)

	for key, _ := range ips {

		targetIp := ips[key]
		recipientAddr, err := net.ResolveUDPAddr("udp", targetIp+":"+PORT)
		logError(err)
		conn, err := net.DialUDP("udp", nil, recipientAddr)
		if !logError(err) {
			conn.Write(b)
			conn.Close()
		}
	}
}

// update the RM on receiving message from leader
func updateRMProtocolHandler(receivedData map[string][]string, myMembers map[string]Entry) {
	for k, v := range receivedData {
		RM.InsertSlice(k, v)
	}
}

//////
func batchkeysProtocolHandler(receivedMessageData interface{}, myKeyValue KeyValue) {
	//fmt.Println("Going to update batch keys")
	for key, value := range receivedMessageData.(map[string]interface{}) {
		myKeyValue.data[key] = value
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
			if receivedValue.Leave == false && receivedValue.Failure == false {
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
	if receivedData.Command == "insert" {
		myKeyValue.Insert(string(receivedData.Key), receivedData.Value)
		fmt.Print("INSERT: ")
		fmt.Print(receivedData.Key)
		fmt.Println(" was inserted from " + receivedData.Origin)
	} else if receivedData.Command == "lookup" {
		message := myKeyValue.Lookup(string(receivedData.Key))
		sendMessageToOrigin(receivedData.Origin, message)
		fmt.Print("LOOKUP: ")
		fmt.Print(receivedData.Key)
		fmt.Println(" was looked up from " + receivedData.Origin)
	} else if receivedData.Command == "update" {
		myKeyValue.Update(string(receivedData.Key), receivedData.Value)
		fmt.Print("UPDATE: ")
		fmt.Print(receivedData.Key)
		fmt.Println(" was updated from " + receivedData.Origin)
	} else if receivedData.Command == "delete" {
		fmt.Println("deleting")
		myKeyValue.Delete(string(receivedData.Key))
		fmt.Print("DELETE: ")
		fmt.Print(receivedData.Key)
		fmt.Println(" was deleted from " + receivedData.Origin)
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

func firstAskContact(members map[string]Entry, selfName string, sock *net.UDPConn) {
	if strings.Split(selfName, "#")[1] == CONTACT_POINT {
		RM_LEADER = CONTACT_POINT
	} else {
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

		// Wait for contact point to respond
		buf := make([]byte, RECV_BUF_LEN)
		rlen, _, err := sock.ReadFromUDP(buf)
		if QUIT == true {
			return
		}
		logError(err)

		//Respond received
		var receivedMessage Message
		err = json.Unmarshal(buf[:rlen], &receivedMessage)

		receivedMembers := convertToEntryMap(receivedMessage.Data)
		gossipProtocolHandler(receivedMembers, members)

		// Update leader pointer
		leaderAskHandler(CONTACT_POINT, strings.Split(selfName, "#")[1])

		// Block for leader pointer
		for {
			buf = make([]byte, RECV_BUF_LEN)
			rlen, _, err = sock.ReadFromUDP(buf)
			if QUIT == true {
				return
			}
			logError(err)

			//Second, setting up member information from retrieved value
			var receivedMessage Message
			err = json.Unmarshal(buf[:rlen], &receivedMessage)

			if receivedMessage.Datatype == "leader-tell" {
				RM_LEADER = receivedMessage.Data.(string)
				break
			}
		}

		// Get contact point's rm
		m = createMessage("rmRequest", strings.Split(selfName, "#")[1])

		b, err = json.Marshal(m)
		memberAddr, err = net.ResolveUDPAddr("udp", CONTACT_POINT+":"+PORT)
		logError(err)
		conn, err = net.DialUDP("udp", nil, memberAddr)
		if !logError(err) {
			conn.Write(b)
			conn.Close()
		}

	}
	//send request to leader
	m := createMessage("fillSparseEntry", SELF_IP)

	b, err := json.Marshal(m)
	memberAddr, err := net.ResolveUDPAddr("udp", RM_LEADER+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, memberAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
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
	key := genericData.(map[string]interface{})["Key"].(string)
	value := genericData.(map[string]interface{})["Value"]

	return KVData{command, origin, key, value}
}

func convertToRM(genericData interface{}) map[string][]string {
	halfRM := genericData.(map[string]interface{})
	retRM := make(map[string][]string)
	for key, _ := range halfRM {
		arr := halfRM[key].([]interface{})
		for index, _ := range arr {
			retRM[key] = append(retRM[key], arr[index].(string))
		}
	}
	return retRM
}
