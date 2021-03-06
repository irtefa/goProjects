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

type KVData struct {
	Command string      `json:"Command"`
	Origin  string      `json:"Origin"`
	Key     string      `json:"Key"`
	Value   interface{} `json:"Value"`
	Version float64     `json:"Version"`
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
func recvHeartBeat(sock *net.UDPConn, myMembers map[string]Entry, selfName string, c chan KVData) {
	for {
		//we should change the byte length in the future
		//First initialize connection
		buf := make([]byte, RECV_BUF_LEN)
		//fmt.Println("before")
		rlen, _, err := sock.ReadFromUDP(buf)
		//fmt.Println("after")
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
			keyValueProtocolHandler(receivedMessageData, myMembers, selfName)
		} else if receivedMessage.Datatype == "kvresp" {
			//This handler is mainly just for testing client-stuff
			receivedMessageData := convertToKVData(receivedMessage.Data)

			//c <- receivedMessageData

			select {
			case c <- receivedMessageData:
			default:
				//fmt.Print("WARNING: Message received but not parsed | ")
				//fmt.Println(receivedMessageData)
			}
		} else if receivedMessage.Datatype == "string" {
			fmt.Println(receivedMessage.Data.(string))
		} else if receivedMessage.Datatype == "batchkeys" {
			batchkeysProtocolHandler(receivedMessage.Data)
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
			requestValueHandler(receivedMessage.Data.(string))
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

	// 1) Iterate through the ipAdressses of a given key
	//		if ip address exists and is not failed, do nothing
	//		else, remove it

	// 2) Using remainder, find new ones.

	for key, ipAddrs := range rmData {
		goodIps := make([]string, 0)

		// After this, leftover ips is all good ips left
		for index, _ := range ipAddrs {
			ipAddress := ipAddrs[index]

			for member, entry := range myMembers {
				memberIp := strings.Split(member, "#")[1]

				if memberIp == ipAddress && entry.Failure == false {
					goodIps = append(goodIps, ipAddress)
				} else {
					// Do nothing
				}
			}
		}

		oldIps := goodIps
		// Find replacements if needed
		leftoverAmt := len(goodIps)

		replacements := pickAdressesFilterThese(myMembers, REPLICA_LEVEL-leftoverAmt, goodIps)

		for i, _ := range replacements {
			replacementIp := strings.Split(replacements[i], "#")[1]
			goodIps = append(goodIps, replacementIp)
		}

		RM.Replace(key, goodIps)

		//iterate through good ips
		for i, _ := range goodIps {
			isReplace := false
			goodIp := goodIps[i]

			for j, _ := range replacements {
				replacementIp := strings.Split(replacements[j], "#")[1]
				if goodIp == replacementIp {
					isReplace = true
				}
			}

			if isReplace {
				kvMsg := createMessage("askforvalue", goodIp+"#"+key)
				b, _ := json.Marshal(kvMsg)
				recipientAddr, err := net.ResolveUDPAddr("udp", oldIps[0]+":"+PORT)
				logError(err)
				conn, err := net.DialUDP("udp", nil, recipientAddr)
				if !logError(err) {
					conn.Write(b)
					conn.Close()
				}
			}
		}
	}

	for member, _ := range myMembers {
		memberIp := strings.Split(member, "#")[1]

		if memberIp == SELF_IP {
		} else {
			rmRequestHandler(memberIp)
		}
	}
	// only rm Request if a change was made, and to every ip in our membership
}

func requestValueHandler(receivedMessage string) {
	targetIp := strings.Split(receivedMessage, "#")[0]
	key := strings.Split(receivedMessage, "#")[1]
	kvData := KVData{"insert", SELF_IP, key, MY_KEY_VALUE.Lookup(key), 1}
	value := createMessage("keyvalue", kvData)
	b, _ := json.Marshal(value)
	//fmt.Print("++++")
	//fmt.Print(key)
	//fmt.Println("++++")
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
		fmt.Println(key)
		if len(value) < REPLICA_LEVEL {
			ips := RM.GetAll(key)
			for idx, _ := range ips {
				targetIp := ips[idx]
				//send request here with ip address and key
				kvMsg := createMessage("askforvalue", recipient_IP+"#"+key)
				b, _ := json.Marshal(kvMsg)
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

	//rmRequestHandler(recipient_IP)
	for member, _ := range myMembers {
		memberIp := strings.Split(member, "#")[1]

		if memberIp == SELF_IP {
		} else {
			rmRequestHandler(memberIp)
		}
	}
}

func rmRequestHandler(requestor_ip string) {
	sendData := make(map[string][]string)
	sendData = RM.GetEntireRmData()

	for key, value := range sendData {
		sendChunk := make(map[string][]string)
		sendChunk[key] = value
		mappingMsg := createMessage("updateRM", sendChunk)
		b, _ := json.Marshal(mappingMsg)

		recipientAddr, err := net.ResolveUDPAddr("udp", requestor_ip+":"+PORT)
		logError(err)
		conn, err := net.DialUDP("udp", nil, recipientAddr)
		if !logError(err) {
			conn.Write(b)
			conn.Close()
		}
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

	for key, entry := range myMembers {
		if entry.Failure == false {

			ip := strings.Split(key, "#")[1]
			suffix := strings.Split(ip, ".")[3]
			if suffix > leaderSuffix {
				leader = ip
				leaderSuffix = suffix
			}
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

func amITheLeader(myMembers map[string]Entry) {
	leaderSuffix := "1"
	leader := ""

	for key, entry := range myMembers {
		if entry.Failure == false {
			ip := strings.Split(key, "#")[1]
			suffix := strings.Split(ip, ".")[3]
			if suffix > leaderSuffix {
				leader = ip
				leaderSuffix = suffix
			}
		}
	}

	RM_LEADER = leader

	if SELF_IP == leader {
		crashed_ip := RM_LEADER
		RM_LEADER = SELF_IP
		for member, _ := range myMembers {
			ip := strings.Split(member, "#")[1]
			leaderTellHandler(ip)
		}
		crashHandler(crashed_ip, myMembers)
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
		RM.Replace(k, v)
	}
}

//////
func batchkeysProtocolHandler(receivedMessageData interface{}) {
	//fmt.Println("Going to update batch keys")
	for key, value := range receivedMessageData.(map[string]interface{}) {
		MY_KEY_VALUE.Insert(key, value)
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

				CONNECTED_MACHINES += 1

				//log joins
				fmt.Print("JOIN:")
				fmt.Print(receivedKey + " joined the system ")
				fmt.Println(time.Now())
			}
		}
	}
}

func keyValueProtocolHandler(receivedData KVData, myMembers map[string]Entry, selfName string) {
	var value interface{}
	value = 0

	if receivedData.Command == "insert" {
		MY_KEY_VALUE.Insert(string(receivedData.Key), receivedData.Value)
		fmt.Print("INSERT: ")
		fmt.Print(receivedData.Key)
		fmt.Println(" was inserted from " + receivedData.Origin)
		value = receivedData.Value
	} else if receivedData.Command == "lookup" {
		value = MY_KEY_VALUE.Lookup(string(receivedData.Key))
		fmt.Print("LOOKUP: ")
		fmt.Print(receivedData.Key)
		fmt.Println(" was looked up from " + receivedData.Origin)
	} else if receivedData.Command == "update" {
		MY_KEY_VALUE.Update(string(receivedData.Key), receivedData.Value)
		fmt.Print("UPDATE: ")
		fmt.Print(receivedData.Key)
		fmt.Println(" was updated from " + receivedData.Origin)
		value = receivedData.Value
	} else if receivedData.Command == "delete" {
		fmt.Println("deleting")
		MY_KEY_VALUE.Delete(string(receivedData.Key))
		fmt.Print("DELETE: ")
		fmt.Print(receivedData.Key)
		fmt.Println(" was deleted from " + receivedData.Origin)
	}

	//create KVData
	respKVData := KVData{receivedData.Command, SELF_IP, receivedData.Key, value, MY_KEY_VALUE.GetVersion(receivedData.Key)}
	sendMessageToOrigin(receivedData.Origin, respKVData)
}

func sendMessageToOrigin(targetIp string, sendData KVData) {
	m := createMessage("kvresp", sendData)
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
	version := genericData.(map[string]interface{})["Version"].(float64)

	return KVData{command, origin, key, value, version}
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
