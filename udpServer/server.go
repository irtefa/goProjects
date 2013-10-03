/*
 * Server receives the heartBeat and figures out what to do with it
 */

package main

import (
	"encoding/json"
	"fmt"
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

var QUIT bool = false

//our individual entry in heartBeat
type Entry struct {
	Hbc       int64 `json:"Hbc"`
	Timestamp int64 `json:"Timestamp"`
	Failure   bool  `json:"Failure"`
}

func main() {
	sock := netSetup()

	idleLoop()
	members, selfName := initializeMembers(os.Args[1])

	// Joined for loop
	for {
		gameLoop(sock, members, selfName)
		idleLoop()
		sock = netSetup()
		members, selfName = initializeMembers(os.Args[1])
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

	//log initialization
	fmt.Print("START:")
	fmt.Print(selfName + " started ")
	fmt.Println(time.Now())

	notifyContactPoint(members, selfName)
	return members, selfName
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

/*
 * Log error if any
 */
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

/*
1) Receive hearbeats
2) Update hbc
3) Update membership list
4) Send heartbeats to k random members in list
*/
func gameLoop(sock *net.UDPConn, members map[string]Entry, selfName string) {
	go recvHeartBeat(sock, members)
	go checkForExit(sock)

	for {
		// Check if quit
		if QUIT == true {
			return
		}

		//update hbc
		entry := members[selfName]
		entry.Hbc += 1
		entry.Timestamp = time.Now().Unix()
		members[selfName] = entry

		checkFailure(members)
		sendHeartBeat(members, selfName)
		time.Sleep(2000 * time.Millisecond)
	}
}

func checkForExit(sock *net.UDPConn) {
	for {
		userInput := handleCmdInput()

		if strings.ToUpper(userInput) == "LEAVE" {
			fmt.Print("EXIT:Exited program ")
			fmt.Println(time.Now())
			sock.Close()
			QUIT = true
			return
		}
	}
}

// mark failure if time.now - entry.timestamp > 5
func checkFailure(members map[string]Entry) {
	for member, _ := range members {
		entry := members[member]
		if (time.Now().Unix() - entry.Timestamp) >= 5 {
			if !entry.Failure {
				entry.Failure = true
				members[member] = entry
				//log mark failure
				fmt.Print("FAILURE:")
				fmt.Print(member + " is marked as failure ")
				fmt.Println(time.Now())
			}
		}
		if (time.Now().Unix() - entry.Timestamp) >= 10 {
			delete(members, member)
			//log delete
			fmt.Print("DELETE:")
			fmt.Print(member + " is deleted from the members list ")
			fmt.Println(time.Now())
		}
	}
}
func idleLoop() {
	// Check for rejoin. from cmd
	for {
		userInput := handleCmdInput()
		userInput = strings.ToUpper(userInput)
		if userInput == "JOIN" {
			QUIT = false
			return
		} else if userInput == "LEAVE" {
			fmt.Print("EXIT:Exited program ")
			fmt.Println(time.Now())
			os.Exit(0)
		} else {
			fmt.Print("INPUTERROR:Incorrect input ")
			fmt.Println(time.Now())
		}
	}
}

/*
 * handle command line input
 */
func handleCmdInput() string {
	var userInput string
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
	for {
		//we should change the byte length in the future
		buf := make([]byte, RECV_BUF_LEN)

		rlen, _, err := sock.ReadFromUDP(buf)
		if QUIT == true {
			return
		}
		logError(err)

		//read from socket => newList
		//receivedMembers := []Message{}
		var receivedMembers map[string]Entry
		err = json.Unmarshal(buf[:rlen], &receivedMembers)
		//logError(err)
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
			if myValue, exists := myMembers[receivedKey]; exists {
				// Compare the hbc
				if receivedValue.Hbc > myValue.Hbc {
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
				//log joins
				fmt.Print("JOIN:")
				fmt.Print(receivedKey + " joined the system ")
				fmt.Println(time.Now())
			}
		}
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
