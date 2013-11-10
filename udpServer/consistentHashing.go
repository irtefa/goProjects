package main

import (
	"hash/crc32"
	"math"
	"strings"
)

/*
 * create hash for machine
 */
func createHash(machineName string) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(machineName))
	v := h.Sum32()
	//fmt.Println(v)
	return v
}

/*
 * find the successor machine and return its name (ipaddress#timestamp) return true if it should be handled locally
 */
func findSuccessor(k uint32, selfName string, members map[string]Entry) (string, uint32) {
	var firstMachineHash uint32 = math.MaxUint32
	var firstMachineName string
	var successorName string = "none"
	//setup hashing
	h := crc32.NewIEEE()
	//find machine's hash
	selfIp := strings.Split(selfName, "#")[1]
	h.Write([]byte(selfIp))
	successorHash := h.Sum32() //assign itself to successorHash
	//selfHash := successorHash
	//find the next biggest number if there is none, return 0
	for key, _ := range members {
		memberIp := strings.Split(key, "#")[1]
		v := createHash(memberIp)
		//check for the successor
		if v > k {
			successorHash = v
			successorName = key
		}
		//update smallest machine
		if v < firstMachineHash {
			firstMachineHash = v
			firstMachineName = key
		}
	}

	//if no bigger machine return the smallest
	if successorName == "none" {
		return firstMachineName, firstMachineHash
	} else if successorName == selfName {
		return selfName, successorHash
	} else {
		return successorName, successorHash
	}
}
