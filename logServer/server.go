package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
)

const (
	RECV_BUF_LEN = 1024
	PORT         = "8008"
)

func main() {
	fmt.Println("Started the logging server")

	listener, err := net.Listen("tcp", ":"+PORT)
	if err != nil {
		println("error listening:", err.Error())
		os.Exit(1)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accept:", err.Error())
			return
		}
		//creates a go routine to execute grep in shell
		go grepMyLog(conn)
	}
}

/*
 * executes grep in the shell and returns the result through a buffer
 * @param conn socket through which the server communicates with the client
 */
func grepMyLog(conn net.Conn) {
	recvBuf := make([]byte, RECV_BUF_LEN)
	_, err := conn.Read(recvBuf)

	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return
	}

	//convert byte array to a string
	n := bytes.Index(recvBuf, []byte{0})
	s := string(recvBuf[:n])

	//read what the log file is
	metaDataInfo := []string{}
	file, _ := os.Open("metadata.txt")
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		metaDataInfo = append(metaDataInfo, scanner.Text())
	}

	//execute grep on the given string
	cmd := exec.Command("grep", s, metaDataInfo[1])

	cmdOut, cmdErr := cmd.Output()

	//check if there is any error in our grep
	if cmdErr != nil {
		fmt.Println("ERROR WHILE READING")
		fmt.Println(cmdErr)
	}

	//send the results back
	var sendBuf []byte
	if len(cmdOut) > 0 {
		results := metaDataInfo[0] + "\n" + string(cmdOut)
		sendBuf = make([]byte, len(results))
		copy(sendBuf, string(results))
	} else {
		copy(recvBuf, "nothing from "+metaDataInfo[0])
	}
	conn.Write(sendBuf)
	conn.Close()
}
