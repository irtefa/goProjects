/*
 * Server receives the heartBeat and figures out what to do with it
 */

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

//some useful constants
const (
	RECV_BUF_LEN  = 1024
	PORT          = "8000"
	CONTACT_POINT = "127.0.0.1"
	K             = 3
)

type Message struct {
	key       string `json:"key"`
	hbc       int64  `json:"hbc"`
	failure   bool   `json:"failure"`
	timestamp int64  `json:"timestamp"`
}

//our individual entry in heartBeat
type Entry struct {
	hbc       int64
	timestamp int64
	failure   bool
}

func main() {
	log.Println("Started udp daemon")

	//Setup address
	addr, err := net.ResolveUDPAddr("udp", ":"+PORT)
	logError(err)

	//Setup socket
	sock, err := net.ListenUDP("udp", addr)
	logError(err)

	members, selfName := initializeMembers(os.Args[1])
	// Joined for loop
	for {
		gameLoop(sock, members, selfName)
		idleLoop()
	}
}

/*
 * Initialize members list with the self ip
 */
func initializeMembers(ip string) (map[string]Entry, string) {
	t0 := time.Now().Unix()

	//create machine name with time#address
	selfName := fmt.Sprint(t0, "#", ip)
	fmt.Println(selfName)

	//initialize Entry
	entry := Entry{0, t0, false}

	//update list with self
	var members map[string]Entry
	members = make(map[string]Entry)
	members[selfName] = entry
	return members, selfName
}

/*
 * Log error if any
 */
func logError(err error) bool {
	if err != nil {
		log.Println(err)
		return true
	}
	return false
}

/*
1) Receive hearbeats
2) Update hbc
3) Update membership list
4) Send heartbeats to k random members in list
*/
func gameLoop(sock *net.UDPConn, members map[string]Entry, selfName string) {
	go recvHeartBeat(sock, members)

	for {
		//update hbc
		entry := members[selfName]
		entry.hbc += 1
		entry.timestamp = time.Now().Unix()
		members[selfName] = entry

		checkFailure(members)
		sendHeartBeat(members, selfName)
		time.Sleep(2000 * time.Millisecond)

		for member, _ := range members {
			fmt.Print(member)
			fmt.Print("=")
			fmt.Println(members[member])
		}
	}
}

// mark failure if time.now - entry.timestamp > 5
func checkFailure(members map[string]Entry) {
	for member, _ := range members {
		entry := members[member]
		if (time.Now().Unix() - entry.timestamp) >= 5 {
			entry.failure = true
			members[member] = entry
		}
	}
}
func idleLoop() {
	// Check for rejoin. from cmd
}

/*
 * handle command line input
 */
func handleCmdInput() string {
	var userInput string
	fmt.Print("Type something:")
	fmt.Scanf("%s", &userInput)
	fmt.Println("You wrote:", userInput)

	return userInput
}

/*
 * receives heartBeats from other machines
 * updates timestamps
 * @param rlen length of the message received
 * @param remote address of the machine that sent the heartBeat
 * @param buf the byte array containing the messages
 */
func recvHeartBeat(sock *net.UDPConn, myMembers map[string]Entry) {
	sock.Close()
	for {
		addr, err := net.ResolveUDPAddr("udp", ":"+PORT)
		logError(err)

		//Setup socket
		sock, err = net.ListenUDP("udp", addr)
		logError(err)
		//we should change the byte length in the future
		buf := make([]byte, RECV_BUF_LEN)
		rlen, _, err := sock.ReadFromUDP(buf)
		logError(err)

		//read from socket => newList
		//receivedMembers := []Message{}
		var receivedMembers map[string]Entry
		err = json.Unmarshal(buf[:rlen], &receivedMembers)
		//logError(err)
		if err != nil {
			log.Println(err)
			log.Println("unmarshalling failed")
		}
		//compare newList to mylist
		//	1) if higher hbc, update mylist with new hbc and new timestamp
		//	2) else, do nothing
		for receivedKey, _ := range receivedMembers {
			receivedValue := receivedMembers[receivedKey]
			fmt.Println(receivedKey)
			if myValue, exists := myMembers[receivedKey]; exists {
				fmt.Println("coming here")
				// Compare the hbc
				if receivedValue.hbc > myValue.hbc {

					//myValue.hbc = receivedValue.hbc
					//myValue.timestamp = time.Now().Unix()
					//myValue.failure = false
					receivedValue.timestamp = time.Now().Unix()
					receivedValue.failure = false
					myMembers[receivedKey] = receivedValue
				}
			} else {
				var entry Entry
				entry.failure = false
				entry.hbc = receivedValue.hbc
				entry.timestamp = time.Now().Unix()
				myMembers[receivedKey] = entry
			}
		}
		sock.Close()
	}
}

/*
 * picks upto k random addresses to send heartBeats to
 * @param k number of addresses to pick
 */
func pickAdresses(members map[string]Entry, k int, selfName string) []string {
	var aliveMembers []string
	var kMembers []string
	//pick k alive processes
	for key, _ := range members {
		entry := members[key]
		if !entry.failure && key != selfName {
			aliveMembers = append(aliveMembers, key)
		}
	}
	//shuffle
	n := len(aliveMembers)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	randomIntArray := r.Perm(n)

	j := 0
	for i := range randomIntArray {
		if j == k {
			return kMembers
		} else {
			kMembers = append(kMembers, aliveMembers[i])
			j++
		}
	}
	return kMembers
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
		member := kMembers[i]

		//split to timestamp and ip address
		a := strings.Split(member, "#")
		//memberIp = ip
		memberIp := a[1]
		//retrieve a UDPaddr
		memberAddr, err := net.ResolveUDPAddr("udp", memberIp+":"+"9000")
		logError(err)
		//
		conn, err := net.DialUDP("udp", nil, memberAddr)
		if !logError(err) {
			conn.Write(b)
			conn.Close()
		}
	}
}
