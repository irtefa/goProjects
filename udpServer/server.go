/*
 * Server receives the heartBeat and figures out what to do with it
 */

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

//some useful constants
const (
	RECV_BUF_LEN = 1024
	PORT         = "8000"
	K            = 1
)

var (
	QUIT                  bool       = false
	RANDOM_NUMBERS        *rand.Rand = rand.New(rand.NewSource(time.Now().Unix()))
	CONTACT_POINT                    = "192.17.11.40"
	FIRST_GOSSIP_RECIEVED            = false
)

func main() {
	CONTACT_POINT = os.Args[2]
	ip_addr_curr_machine := os.Args[1]
	myKeyValue := KeyValue{}
	myKeyValue.data = make(map[uint32]interface{})

	idleLoop()
	sock, members, selfName := joinLogic(ip_addr_curr_machine, myKeyValue)

	for {
		gameLoop(sock, members, selfName, myKeyValue)
		leaveLogic(selfName, myKeyValue, members)
		idleLoop()
		sock, members, selfName = joinLogic(ip_addr_curr_machine, myKeyValue)
	}
}

func joinLogic(ip_addr_curr_machine string, myKeyValue KeyValue) (*net.UDPConn, map[string]Entry, string) {
	sock := netSetup()

	FIRST_GOSSIP_RECIEVED = false
	QUIT = false

	membershipInfo := initializeMembers(ip_addr_curr_machine)
	members := membershipInfo.List
	selfName := membershipInfo.Id
	notifyContactPoint(members, selfName)

	return sock, members, selfName
}

func leaveLogic(selfName string, myKeyValue KeyValue, members map[string]Entry) {
	selfIp := strings.Split(selfName, "#")[1]
	hashedSelfIp := createHash(selfIp)
	successorName, _ := findSuccessor(hashedSelfIp, selfName, members)
	successorIp := strings.Split(successorName, "#")[1]

	var SendKeyValue map[string]interface{}
	SendKeyValue = make(map[string]interface{})

	var deleteKeyValue map[uint32]interface{}
	deleteKeyValue = make(map[uint32]interface{})

	for key, _ := range myKeyValue.data {
		SendKeyValue[strconv.Itoa(int(key))] = myKeyValue.data[key]
		deleteKeyValue[key] = myKeyValue.data[key]
	}

	//delete the keys that were added to sendKeyValue

	for key, _ := range deleteKeyValue {
		fmt.Print("KEYVALUE: Transferred ")
		fmt.Print(key)
		fmt.Println(" to " + successorIp)
		delete(myKeyValue.data, key)
	}

	//send sendKeyValue over the network
	m := createMessage("batchkeys", SendKeyValue)

	b, err := json.Marshal(m)

	recipientAddr, err := net.ResolveUDPAddr("udp", successorIp+":"+PORT)
	logError(err)
	conn, err := net.DialUDP("udp", nil, recipientAddr)
	if !logError(err) {
		conn.Write(b)
		conn.Close()
	}
}

func requestKeys(selfName string, members map[string]Entry) {
	selfIp := strings.Split(selfName, "#")[1]
	hashedSelfIp := createHash(selfIp)
	successorName, _ := findSuccessor(hashedSelfIp, selfName, members)
	targetIp := strings.Split(successorName, "#")[1]

	m := createMessage("requestkv", selfIp)
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
func gameLoop(sock *net.UDPConn, members map[string]Entry, selfName string, myKeyValue KeyValue) {
	go recvHeartBeat(sock, members, selfName, myKeyValue)
	go checkForExit(sock, members, selfName, myKeyValue)
	var waitDuration int64 = 100

	for {
		startTime := time.Now().Unix()
		// Check if quit
		if QUIT == true {
			entry := members[selfName]
			entry.Leave = true
			entry.Timestamp = time.Now().Unix()
			members[selfName] = entry
			sendHeartBeat(members, selfName)
			return
		}

		//if FIRST_GOSSIP_RECIEVED == true {
		requestKeys(selfName, members)
		//}

		//update hbc
		entry := members[selfName]
		entry.Hbc += 1
		entry.Timestamp = time.Now().Unix()
		members[selfName] = entry

		checkFailure(members)
		sendHeartBeat(members, selfName)

		//Wait proper amount
		remainingTime := waitDuration - (time.Now().Unix() - startTime)
		time.Sleep(time.Duration(remainingTime) * time.Millisecond)
	}
}

func checkForExit(sock *net.UDPConn, members map[string]Entry, selfName string, myKeyValue KeyValue) {
	for {
		userInput := handleCmdInput()
		commands := strings.Fields(userInput) //splits the input into an array

		switch command := strings.ToUpper(commands[0]); {
		case command == "LEAVE":
			{
				fmt.Print("LEAVE:Left the system ")
				fmt.Println(time.Now())
				sock.Close()
				QUIT = true
				return
			}
		case command == "INSERT":
			{
				key, _ := strconv.Atoi(commands[1])
				value := commands[2]
				targetIp := strings.Split(selfName, "#")[1]

				kvdata := KVData{"insert", targetIp, uint32(key), value}
				sendKV(targetIp, kvdata)
			}
		case command == "LOOKUP":
			{
				key, _ := strconv.Atoi(commands[1])
				targetIp := strings.Split(selfName, "#")[1]

				kvdata := KVData{"lookup", targetIp, uint32(key), 0}
				sendKV(targetIp, kvdata)
			}
		default:
			{
				fmt.Println("Incorrect command")
			}
		}
	}
}

// mark failure if time.now - entry.timestamp > 5
func checkFailure(members map[string]Entry) {
	for member, _ := range members {
		entry := members[member]
		if (time.Now().Unix() - entry.Timestamp) >= 5 {
			if entry.Leave {
				delete(members, member)
				fmt.Print("DELETE:")
				fmt.Print(member + " is deleted from the members list due to leave ")
				fmt.Println(time.Now())
			} else if !entry.Failure {
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
			fmt.Print(member + " is deleted from the members list due to failure")
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
		} else if userInput == "EXIT" {
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
	bio := bufio.NewReader(os.Stdin)
	line, _ := bio.ReadString('\n')
	return strings.TrimSuffix(line, "\n")
}
