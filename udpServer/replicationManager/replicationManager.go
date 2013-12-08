package rm

import (
	"fmt"
)

type Rm struct {
	data map[string][]string
}

func NewRm() *Rm {
	_rm := new(Rm)
	_rm.data = make(map[string][]string)
	return _rm
}

func (_rm Rm) Insert(key string, ipAddress string) {
	_rm.data[key] = append(_rm.data[key], ipAddress)
}

func (_rm Rm) InsertSlice(key string, value []string) {
	_rm.data[key] = value
}

func (_rm Rm) Lookup(key string, index int) string {
	if index >= _rm.SizeOfKey(key) {
		return "out-of-bound"
	}
	return (_rm.data[key])[index]
}

func (_rm Rm) GetAll(key string) []string {
	return _rm.data[key]
}

func (_rm Rm) DeleteKey(key string) {
	delete(_rm.data, key)
}

func (_rm Rm) DeleteIp(key string, ip string) {

}

func (_rm Rm) SizeOfKey(key string) int {
	return len(_rm.data[key])
}

func (_rm Rm) Exists(key string) bool {
	return (_rm.data[key] != nil)
}

func (_rm Rm) Show() {
	for key, value := range _rm.data {
		fmt.Print(key)
		fmt.Print(" = ")
		fmt.Print(value)
		fmt.Println("")
	}
}

func (_rm Rm) GetEntireRmData() map[string][]string {
	return _rm.data
}

func (_rm Rm) Replace(key string, value []string) {
	_rm.data[key] = value
}
