// grep_client
package main

import (
	"fmt"
	"os"
	"net"
	"io/ioutil"
	"bufio"
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
	
	
	if len(os.Args) < 2 {
		fmt.Println("ERROR: No arguments presented")
		os.Exit(1)
	} else {
		c := make(chan string)
		
		for i := 0; i < len(ipList); i++ {
			go writeToServer(ipList[i], os.Args[1], c)
		}
		
		for i := 0; i < len(ipList); i++ {
			serverResult := <-c
			fmt.Println(serverResult)
			fmt.Println("----------")
		}
	}
}

/*
*/

func writeToServer(ipAddr string, message string, c chan string){
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