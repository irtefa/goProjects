// grep_client
package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
)

const (
	PORT = "8008"
)

func main() {
	ipList := []string{}
	file, _ := os.Open("masterlist.txt")
	scanner := bufio.NewScanner(file)

	//Compile list of ip address from masterlist.txt
	for scanner.Scan() {
		var ip_content = scanner.Text()
		ip_content = ip_content + ":" + PORT
		ipList = append(ipList, ip_content)
	}

	if len(os.Args) < 3 {
		fmt.Println("ERROR: Not enough arguments presented")
		os.Exit(1)
	} else {
		c := make(chan string)

		key := os.Args[1]
		value := os.Args[2]

		// Check ^ on key
		if strings.HasPrefix(key, "^") {
			key = key[1:len(key)]
		} else {
			key = "[^:]*" + key
		}

		// Check $ on key
		if strings.HasSuffix(key, "$") {
			key = key[0 : len(key)-1]
		} else {
			key = key + "[^:]*"
		}

		// Check ^ on value
		if strings.HasPrefix(value, "^") {
			value = value[1:len(value)]
		} else {
			value = "[^:]*" + value
		}

		// Check $ on value
		if strings.HasSuffix(value, "$") {
			value = value[0 : len(value)-1]
		} else {
			value = value + "[^:]*"
		}

		serverInput := "^" + key + ":" + value + "$"

		for i := 0; i < len(ipList); i++ {
			go writeToServer(ipList[i], serverInput, c)
		}

		for i := 0; i < len(ipList); i++ {
			serverResult := <-c
			fmt.Println(serverResult)
			fmt.Println("----------")
		}
	}
}

/*
 * Sends a message to a server, and returns the file into a channel
 * @param ipAddr string representation of the server's IP Address
 * @param message the message to be sent back to the server
 * @param c the channel for returning server messages
 */
func writeToServer(ipAddr string, message string, c chan string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", ipAddr)
	if err != nil {
		c <- err.Error()
		return
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		c <- err.Error()
		return
	}

	_, err = conn.Write([]byte(message))
	if err != nil {
		c <- err.Error()
		return
	}

	result, err := ioutil.ReadAll(conn)
	if err != nil {
		c <- err.Error()
		return
	}

	c <- string(result)
}
