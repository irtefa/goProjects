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
	CONTACT_POINT = "192.17.11.40"
	K             = 3
)

//our individual entry in heartBeat
type Entry struct {
	Hbc       int64 `json:"Hbc"`
	Timestamp int64 `json:"Timestamp"`
	Failure   bool  `json:"Failure"`
}

func main() {
	log.Println("Started udp daemon")

	sock := netSetup()

	idleLoop()
	members, selfName := initializeMembers(os.Args[1])

	// Joined for loop
	for {
		gameLoop(sock, members, selfName)
		idleLoop()
	}
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
 * Initialize members list with the self ip
 */
func initializeMembers(ip string) (map[string]Entry, string) {
	t0 := time.Now().Unix()

	//create machine name with time#address
	selfName := fmt.Sprint(t0, "#", ip)

	//initialize Entry
	entry := Entry{0, t0, false}

	//update list with self
	var members map[string]Entry
	members = make(map[string]Entry)
	members[selfName] = entry
	notifyContactPoint(members)
	return members, selfName
}

func notifyContactPoint(members map[string]Entry) {
	b, err := json.Marshal(members)
	//send to contact point
	memberAddr, err := net.ResolveUDPAddr("udp", CONTACT_POINT+":"+PORT)
	logError(err)
	//
	conn, err := net.DialUDP("udp", nil, memberAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}
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
		entry.Hbc += 1
		entry.Timestamp = time.Now().Unix()
		members[selfName] = entry

		checkFailure(members)
		sendHeartBeat(members, selfName)
		time.Sleep(2000 * time.Millisecond)

		/*
			for member, _ := range members {
				fmt.Print(member)
				fmt.Print("=")
				fmt.Println(members[member])
			}*/
	}
}

// mark failure if time.now - entry.timestamp > 5
func checkFailure(members map[string]Entry) {
	for member, _ := range members {
		entry := members[member]
		if (time.Now().Unix() - entry.Timestamp) >= 5 {
			entry.Failure = true
			members[member] = entry
		}
	}
}
func idleLoop() {
	// Check for rejoin. from cmd
	for {
		fmt.Println("Currently not connected to any membership")
		fmt.Println("-------")
		fmt.Println("OPTIONS")
		fmt.Println("-------")
		fmt.Println("1) Join membership (Contact the contact point)")
		fmt.Println("2) Exit program")

		userInput := handleCmdInput()

		if userInput == "1" {
			fmt.Println("Joining contact point...")
			return
		} else if userInput == "2" {
			fmt.Println("Exited program")
			os.Exit(0)
		} else {
			fmt.Println("Incorrect input")
			fmt.Println("********************")
			fmt.Println("********************")
		}
	}
}

/*
 * handle command line input
 */
func handleCmdInput() string {
	var userInput string
	fmt.Print("   Command:")
	fmt.Scanf("%s", &userInput)

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
			fmt.Print(receivedKey)
			fmt.Print(":")
			fmt.Println(receivedValue.Hbc)
			if myValue, exists := myMembers[receivedKey]; exists {
				//fmt.Println("coming here")
				// Compare the hbc
				if receivedValue.Hbc > myValue.Hbc {

					//myValue.hbc = receivedValue.hbc
					//myValue.timestamp = time.Now().Unix()
					//myValue.failure = false
					receivedValue.Timestamp = time.Now().Unix()
					receivedValue.Failure = false
					myMembers[receivedKey] = receivedValue
				}
			} else {
				var entry Entry
				entry.Failure = false
				entry.Hbc = receivedValue.Hbc
				entry.Timestamp = time.Now().Unix()
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
		if !entry.Failure && key != selfName {
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

	/*
		var receivedMembers map[string]Entry
		err = json.Unmarshal(b[:], &receivedMembers)
		for member, _ := range receivedMembers {
			fmt.Print(member)
			fmt.Print("=")
			fmt.Println(receivedMembers[member])
		}*/
}
