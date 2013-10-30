package main

import (
	"fmt"
	"time"
)

type Membership struct {
	Id   string
	List map[string]Entry
}

//our individual entry in heartBeat
type Entry struct {
	Hbc       int64 `json:"Hbc"`
	Timestamp int64 `json:"Timestamp"`
	Failure   bool  `json:"Failure"`
	Leave     bool  `json:"Leave"`
}

/*
 * Initialize members list with the self ip
 */
func initializeMembers(ip string) Membership {
	//create machine name with time#address
	t0 := time.Now().Unix()
	selfName := fmt.Sprint(t0, "#", ip)

	//initialize Entry
	entry := Entry{0, t0, false, false}

	//update list with self
	var members map[string]Entry
	members = make(map[string]Entry)
	members[selfName] = entry

	//log initialization
	fmt.Print("START:")
	fmt.Print(selfName + " started ")
	fmt.Println(time.Now())

	notifyContactPoint(members, selfName)

	membershipInfo := Membership{selfName, members}
	return membershipInfo
}

/*
 * picks upto k random addresses to send heartBeats to
 * @param k number of addresses to pick
 */
func pickAdresses(members map[string]Entry, k int, selfName string) []string {
	var aliveMembers []string
	var kMembers []string
	//pick k alive processes
	for key, _ := range members {
		entry := members[key]
		if !entry.Failure && key != selfName {
			aliveMembers = append(aliveMembers, key)
		}
	}
	//shuffle
	n := len(aliveMembers)
	r := RANDOM_NUMBERS

	randomIntArray := r.Perm(n)

	j := 0
	for j < k {
		if j >= n {
			return kMembers
		}

		kMembers = append(kMembers, aliveMembers[randomIntArray[j]])
		j++
	}
	return kMembers
}
