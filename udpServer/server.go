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
    RECV_BUF_LEN = 1024
    PORT = "8000"
    MASTER_LIST = "masterlist.txt"
)

//our individual entry in heartBeat
type Message struct {
    Body string
    Time int64
}

func main() {
    log.Println("Started udp daemon")
    
    addr, err := net.ResolveUDPAddr("udp", ":" + PORT)
    
    if err != nil {
        log.Println(err)
    }
    
    sock, err := net.ListenUDP("udp", addr)
    
    if err != nil {
        log.Println(err)
    }
    
    for {
        //we should change the byte length in the future
        buf := make([]byte, RECV_BUF_LEN)

        rlen, remote, err := sock.ReadFromUDP(buf)
        if err != nil {
            log.Println(err)
        }  
        //recieve heartBeat
        go recvHeartBeat(rlen, remote, buf)     
    }
}

/*
 * receives heartBeats from other machines
 * updates timestamps
 * @param rlen length of the message received
 * @param remote address of the machine that sent the heartBeat
 * @param buf the byte array containing the messages
 */
func recvHeartBeat(rlen int, remote *net.UDPAddr, buf [] byte) {
    //print message length
    log.Println(rlen)
    //print client address
    log.Println(remote)
    //print message sent
    log.Println(string(buf))
}
