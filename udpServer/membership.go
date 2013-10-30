package main

import (
	"fmt"
	"time"
)

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
func initializeMembers(ip string) (map[string]Entry, string) {
	t0 := time.Now().Unix()

	//create machine name with time#address
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
	return members, selfName
}

func compareMembers(inputKey string, inputValue Entry, storedValue Entry, storedMembersList map[string]Entry) {
	if inputValue.Leave == true {
		if storedMembersList[inputKey].Leave == false {
			entry := storedMembersList[inputKey]
			entry.Leave = true
			entry.Timestamp = time.Now().Unix()
			storedMembersList[inputKey] = entry

			//log leaves
			fmt.Print("LEAVE:")
			fmt.Print(inputKey + " left the system ")
			fmt.Println(time.Now())
		}
	} else if inputValue.Hbc > storedValue.Hbc {
		if storedMembersList[inputKey].Leave == false {
			inputValue.Timestamp = time.Now().Unix()
			inputValue.Failure = false
			storedMembersList[inputKey] = inputValue
		}
	}
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
