package rm

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

func (_rm Rm) Lookup(key string, index int) string {
	if index >= _rm.SizeOfKey(key) {
		return "out-of-bound"
	}
	return (_rm.data[key])[index]
}

func (_rm Rm) DeleteKey(key string) {
	delete(_rm.data, key)
}

func (_rm Rm) SizeOfKey(key string) int {
	return len(_rm.data[key])
}
