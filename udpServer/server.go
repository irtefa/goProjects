/*
 * Server receives the heartBeat and figures out what to do with it
 */

package main

import (
	"log"
	"net"
)

//some useful constants
const (
	RECV_BUF_LEN  = 1024
	PORT          = "8000"
	MASTER_LIST   = "masterlist.txt"
	CONTACT_POINT = "127.0.0.1"
)

//our individual entry in heartBeat
type Message struct {
	Body string
	Time int64
}

func main() {
	log.Println("Started udp daemon")

	addr, err := net.ResolveUDPAddr("udp", ":"+PORT)

	if err != nil {
		log.Println(err)
	}

	sock, err := net.ListenUDP("udp", addr)
	//initialize time
	//initialize heartbeat
	//create machine name with time#address
	//update list with self
	if err != nil {
		log.Println(err)
	}

	// Joined for loop
	for {
		//we should change the byte length in the future
		buf := make([]byte, RECV_BUF_LEN)

		rlen, remote, err := sock.ReadFromUDP(buf)
		if err != nil {
			log.Println(err)
		}
		//recieve heartBeat
		go recvHeartBeat(rlen, remote, buf)
		//update heartBeat counter -> also update self in membership list
		//pick random addresses to send heartbeats
	}
}

/*
 * handle command line input
 */
func handleCmdInput() {

}

/*
 * receives heartBeats from other machines
 * updates timestamps
 * @param rlen length of the message received
 * @param remote address of the machine that sent the heartBeat
 * @param buf the byte array containing the messages
 */
func recvHeartBeat(rlen int, remote *net.UDPAddr, buf []byte) {
	//print message length
	log.Println(rlen)
	//print client address
	log.Println(remote)
	//print message sent
	log.Println(string(buf))

	//for each entry
	//if not fail
	//if machine does not exist
	//add it to memebership list
	//else
	//if self.hbc < entry.hbc
	//self.hbc = entry.hbc
	//update timestamp
}

/*
 * picks k random addresses to send heartBeats to
 * @param i heartBeat counter
 */
func pickAdresses(i int) {

}

/*
 * check if entry.timestamp - currentTimeStamp > 5s
 */
func checkForFailure() {

}

/*
 * sends heartbeats regularly
 * to machines in the system
 * @param conn the socket
 * @param message heartBeat, failure, timestamp
 */
func sendHeartBeat(conn net.Conn, message []byte) {

}
