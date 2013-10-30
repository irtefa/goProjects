package main

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
)

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
	b, err := json.Marshal(members)
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
		//
		conn, err := net.DialUDP("udp", nil, recipientAddr)
		if !logError(err) {
			conn.Write(b)
			conn.Close()
		}
	}
}

/*
 * receives heartBeats from other machines
 * updates timestamps
 * @param rlen length of the message received
 * @param remote address of the machine that sent the heartBeat
 * @param buf the byte array containing the messages
 */
func recvHeartBeat(sock *net.UDPConn, myMembers map[string]Entry) {
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
		var receivedMembers map[string]Entry
		err = json.Unmarshal(buf[:rlen], &receivedMembers)
		if err != nil {
			fmt.Print("MARSHALFAIL:")
			fmt.Print(err)
			fmt.Println(time.Now())
		}

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
}

func notifyContactPoint(members map[string]Entry, selfName string) {
	b, err := json.Marshal(members)
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
