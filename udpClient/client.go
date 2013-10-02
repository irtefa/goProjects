/*
 * Client basically sends the heartBeat
 */

package main

import (
    "bufio"
    "encoding/json" //this is not final we could come up with something more lightweight
    "log"
    "net"
    "os"
)

//some useful constants
const (
    RECV_BUF_LEN = 1024
    PORT = "8000"
    HOST_LIST = "hostlist.txt"
    MASTER_LIST = "masterlist.txt" //we can change this name
)

//our individual entry in heartBeat
type Message struct {
    Body string
    Time int64
}

func main() {
    file, _ := os.Open(HOST_LIST)
    scanner := bufio.NewScanner(file)
    
    for scanner.Scan() {
        hostName := scanner.Text()
        //right now connecting to only one server
        serverAddr, err := net.ResolveUDPAddr("udp", hostName + ":" + PORT)
        
        if err != nil {
            log.Println(err)
        } else {
            conn, err := net.DialUDP("udp",nil, serverAddr)
            //could not connect to the server
            if err != nil { 
                log.Println(err)
            } else {
                c := make(chan [] byte)
                //send a simple message to the server
                go sendHeartBeat(conn, c)
                conn.Write(<-c)
                conn.Close()
            }
        }
    }
}
/*
func connectWithHost(hostName string) {
    //right now connecting to only one server
    serverAddr, err := net.ResolveUDPAddr("udp", hostName + ":" + PORT)
    
    if err != nil {
        log.Println(err)
    } else {
        conn, err := net.DialUDP("udp",nil, serverAddr)
        //could not connect to the server
        if err != nil { 
            log.Println(err)
        } else {
            c := make(chan [] byte)
            //send a simple message to the server
            go sendHeartBeat(conn, c)
            conn.Write(<-c)
            conn.Close()
        }
    }
}
/*
 * sends heartbeats regularly
 * to machines in the system
 * @param conn the socket
 * @param c    the channel for returning heartBeat messages
 */
func sendHeartBeat(conn net.Conn, c chan [] byte) {
    heartBeat := []Message{}

    //read all the timestamps from the file
    file, _ := os.Open(MASTER_LIST)
    scanner := bufio.NewScanner(file)

    //we read the records from the masterlist
    i := 0
    for scanner.Scan() {
        machineTimestamp := scanner.Text()
        heartBeat = append(heartBeat, Message{machineTimestamp, int64(i)})
        i++
    }

    //encode heartBeat to json
    b, err := json.Marshal(heartBeat)

    if err != nil {
        log.Println(err)
    }

    //instead of printing this should be sending heartBeats to multiple machines
    c <- b
}
