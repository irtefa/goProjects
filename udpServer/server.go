package main

import (
    "fmt"
    "net"
)

func main() {
    fmt.Println("Started udp daemon")
    
    addr, err := net.ResolveUDPAddr("udp", ":8000")
    
    if err != nil {
        fmt.Println(err)
    }
    
    sock, err := net.ListenUDP("udp", addr)
    
    if err != nil {
        fmt.Println(err)
    }
    
    for {
        //we should change the byte length in the future
        buf := make([]byte, 1024)
        rlen, remote, err := sock.ReadFromUDP(buf)
        if err != nil {
            fmt.Println(err)
        }
        //print client address
        fmt.Println(remote)
        //print length of the string sent
        fmt.Println(rlen)
        //print the string sent
        fmt.Println(string(buf))
    }
}
