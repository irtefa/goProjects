/*
 * Server receives the heartBeat and figures out what to do with it
 */

package main

import (
	"./replicationManager"
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
	RM                        = rm.NewRm()
	REPLICA_LEVEL             = 3
	RM_LEADER                 = "empty"
	SELF_IP                   = "empty"
)

func main() {
	CONTACT_POINT = os.Args[2]
	SELF_IP = os.Args[1]
	myKeyValue := KeyValue{}
	myKeyValue.data = make(map[string]interface{})
	myKeyValue.version = make(map[string]float64)

	idleLoop()
	sock, members, selfName := joinLogic(SELF_IP, myKeyValue)

	for {
		gameLoop(sock, members, selfName, myKeyValue)
		leaveLogic(selfName, myKeyValue, members)
		idleLoop()
		sock, members, selfName = joinLogic(SELF_IP, myKeyValue)
	}
}

func joinLogic(ip_addr_curr_machine string, myKeyValue KeyValue) (*net.UDPConn, map[string]Entry, string) {
	sock := netSetup()

	QUIT = false

	membershipInfo := initializeMembers(ip_addr_curr_machine)
	members := membershipInfo.List
	selfName := membershipInfo.Id

	firstAskContact(members, selfName, sock)

	//notifyContactPoint(members, selfName)
	//leaderAskHandler(CONTACT_POINT, strings.Split(selfName, "#")[1])

	return sock, members, selfName
}

func leaveLogic(selfName string, myKeyValue KeyValue, members map[string]Entry) {
}

/*func requestKeys(selfName string, members map[string]Entry) {
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
}*/

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
	var c chan KVData = make(chan KVData)

	go recvHeartBeat(sock, members, selfName, myKeyValue, c)
	go checkForExit(sock, members, selfName, myKeyValue, c)
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

func checkForExit(sock *net.UDPConn, members map[string]Entry, selfName string, myKeyValue KeyValue, c chan KVData) {
	for {
		userInput := handleCmdInput()
		commands := strings.Fields(userInput) //splits the input into an array

		if len(commands) != 0 {
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
					intlevel := findLevelAmt(commands[1])
					key := commands[2]
					value := commands[3]
					targetIp := strings.Split(selfName, "#")[1]

					kvdata := KVData{"insert", targetIp, key, value, 0}
					sendKV(targetIp, kvdata)
					_ = waitForLevelAmt(intlevel, c)
				}
			case command == "LOOKUP":
				{
					intlevel := findLevelAmt(commands[1])
					key := commands[2]
					targetIp := strings.Split(selfName, "#")[1]

					kvdata := KVData{"lookup", targetIp, key, 0, 0}
					sendKV(targetIp, kvdata)

					response := waitForLevelAmt(intlevel, c)
					fmt.Println(response.Value)
				}
			case command == "DELETE":
				{
					intlevel := findLevelAmt(commands[1])
					key := commands[2]
					targetIp := strings.Split(selfName, "#")[1]

					kvdata := KVData{"delete", targetIp, key, 0, 0}
					sendKV(targetIp, kvdata)
					_ = waitForLevelAmt(intlevel, c)
				}
			case command == "UPDATE":
				{
					intlevel := findLevelAmt(commands[1])
					key := commands[2]
					value := commands[3]
					targetIp := strings.Split(selfName, "#")[1]

					kvdata := KVData{"update", targetIp, key, value, 0}
					sendKV(targetIp, kvdata)
					_ = waitForLevelAmt(intlevel, c)
				}
			case command == "SHOW":
				{
					fmt.Println()
					fmt.Println("Showing all key|values")
					fmt.Println("======================")
					for hashedKey, _ := range myKeyValue.data {
						fmt.Print("key: ")
						fmt.Print(hashedKey)
						fmt.Print(" | value: ")
						fmt.Print(myKeyValue.data[hashedKey])
						fmt.Println("")
					}
					fmt.Println()
					fmt.Println("****")
					fmt.Println()
					fmt.Println("Membership list")
					fmt.Println("======================")
					for membername, _ := range members {
						fmt.Println(membername)
					}
					fmt.Println()
				}
			case command == "RM":
				{
					RM.Show()
				}
			case command == "LEADER":
				{
					fmt.Println(RM_LEADER)
				}
			default:
				{
					fmt.Println("Incorrect command")
				}
			}
		}
	}
}

func findLevelAmt(level string) int {
	if strings.ToUpper(level) == "ONE" {
		return 1
	} else if strings.ToUpper(level) == "QUORUM" {
		return REPLICA_LEVEL/2 + 1
	} else if strings.ToUpper(level) == "ALL" {
		return REPLICA_LEVEL
	} else {
		return 1
	}
}

func waitForLevelAmt(level int, c chan KVData) KVData {
	counter := 0
	finalResult := KVData{"nil", "nil", "nil", "nil", 0}

	for {
		msg := <-c
		if finalResult.Command == "nil" {
			finalResult = msg
		} else if msg.Version > finalResult.Version {
			finalResult = msg
		}
		counter = counter + 1

		if counter == level {
			fmt.Println("*************")
			fmt.Print("Received responses from: ")
			fmt.Print(counter)
			fmt.Println(" RMs")
			break
		}
	}

	return finalResult
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

				if SELF_IP == RM_LEADER {
					crashHandler(strings.Split(member, "#")[1], members)
				} else if RM_LEADER == strings.Split(member, "#")[1] {
					// If the leader had failed
					amITheLeader(members)
				}
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
