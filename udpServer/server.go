/*
 * Server receives the heartBeat and figures out what to do with it
 */

package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
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
	QUIT           bool       = false
	RANDOM_NUMBERS *rand.Rand = rand.New(rand.NewSource(time.Now().Unix()))
	CONTACT_POINT             = "192.17.11.40"
)

func main() {
	CONTACT_POINT = os.Args[2]
	ip_addr_curr_machine := os.Args[1]
	sock := netSetup()

	idleLoop()
	membershipInfo := initializeMembers(ip_addr_curr_machine)

	members := membershipInfo.List
	selfName := membershipInfo.Id
	//initialize keyValue
	myKeyValue := KeyValue{}
	myKeyValue.data = make(map[uint32]interface{})
	// Joined for loop
	for {
		gameLoop(sock, members, selfName, myKeyValue)
		idleLoop()
		sock = netSetup()
		//members, selfName = initializeMembers(ip_addr_curr_machine)
		membershipInfo := initializeMembers(ip_addr_curr_machine)

		members = membershipInfo.List
		selfName = membershipInfo.Id
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
	go recvHeartBeat(sock, members, selfName)
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
		case command == "NEXT":
			{
				name, hash := findSuccessor(selfName, members)
				fmt.Println("---Machine name: " + name)
				fmt.Print("---Machine hash: ")
				fmt.Println(hash)
			}
		case command == "INSERT":
			{
				key := commands[1]
				value := commands[2]
				//insert key value
				myKeyValue.Insert(key, value)
			}
		case command == "LOOKUP":
			{
				key := commands[1]
				fmt.Println(myKeyValue.Lookup(key))
			}
		case command == "TESTKVSEND":
			{
				name, _ := findSuccessor(selfName, members)
				successorIp := strings.Split(name, "#")[1]
				selfIp := strings.Split(selfName, "#")[1]
				sendKV(successorIp, KVData{"insert", selfIp, 325, "stuff"})
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
