package kv

import (
	"fmt"
)

type kvData struct {
	Key     string
	Value   interface{}
	Version float64
	Command string
}

type KeyValue struct {
	data         map[string]interface{}
	version      map[string]float64
	recentReads  []kvData
	recentWrites []kvData
}

func NewKeyValue() *KeyValue {
	_kv := new(KeyValue)
	_kv.data = make(map[string]interface{})
	_kv.version = make(map[string]float64)
	_kv.recentReads = make([]kvData, 10)
	_kv.recentWrites = make([]kvData, 10)

	return _kv
}

func (_kv KeyValue) Insert(key string, value interface{}) {
	_kv.data[key] = value
	_kv.version[key] += 1

	newItem := kvData{key, value, _kv.version[key], "Insert"}
	_kv.addRecentWrite(newItem)
}

func (_kv KeyValue) Lookup(key string) interface{} {
	if _kv.data[key] == nil {
		return "Key not found"
	}
	_kv.version[key] += 1

	newItem := kvData{key, _kv.data[key], _kv.version[key], "Lookup"}
	_kv.addRecentRead(newItem)

	return _kv.data[key]
}

func (_kv KeyValue) Update(key string, newValue interface{}) {
	_kv.data[key] = newValue
	_kv.version[key] += 1

	newItem := kvData{key, newValue, _kv.version[key], "Update"}
	_kv.addRecentWrite(newItem)
}

func (_kv KeyValue) Delete(key string) {
	delete(_kv.data, key)
}

func (_kv KeyValue) GetVersion(key string) float64 {
	return _kv.version[key]
}

func (_kv KeyValue) ReadRecentRead() {
	fmt.Println("")
	fmt.Println("Recent Reads")
	fmt.Println("***rank: Key | Value**")

	for i, _ := range _kv.recentReads {
		fmt.Print(i + 1)
		fmt.Print(":    ")
		fmt.Print(_kv.recentReads[i].Key)
		fmt.Print(" | ")
		fmt.Print(_kv.recentReads[i].Value)
		fmt.Print("-----> Version: ")
		fmt.Print(_kv.recentReads[i].Version)
		fmt.Print(", Command: ")
		fmt.Println(_kv.recentReads[i].Command)
	}
	fmt.Println("")
	fmt.Println("")
}

func (_kv KeyValue) ReadRecentWrite() {
	fmt.Println("")
	fmt.Println("Recent Writes")
	fmt.Println("***rank: Key | Value**")

	for i, _ := range _kv.recentWrites {
		fmt.Print(i + 1)
		fmt.Print(":    ")
		fmt.Print(_kv.recentWrites[i].Key)
		fmt.Print(" | ")
		fmt.Print(_kv.recentWrites[i].Value)
		fmt.Print("-----> Version: ")
		fmt.Print(_kv.recentWrites[i].Version)
		fmt.Print(", Command: ")
		fmt.Println(_kv.recentWrites[i].Command)
	}
	fmt.Println("")
	fmt.Println("")
}

// Private functions

func (_kv KeyValue) addRecentRead(incomingData kvData) {
	next := incomingData
	curr := incomingData

	for i, _ := range _kv.recentReads {
		next = _kv.recentReads[i]
		_kv.recentReads[i] = curr
		curr = next
	}
}

func (_kv KeyValue) addRecentWrite(incomingData kvData) {
	next := incomingData
	curr := incomingData

	for i, _ := range _kv.recentWrites {
		next = _kv.recentWrites[i]
		_kv.recentWrites[i] = curr
		curr = next
	}
}
