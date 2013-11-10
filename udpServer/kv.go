package main

type KVData struct {
	Command string      `json:"Command"`
	Origin  string      `json:"Origin"`
	Key     uint32      `json:"Key"`
	Value   interface{} `json:"Value"`
}

type KeyValue struct {
	data map[uint32]interface{}
}

func (kv KeyValue) Insert(key string, value interface{}) {
	//stringyKey := fmt.Sprintf("%s", key)
	intKey := createHash(key)
	kv.data[intKey] = value
}

func (kv KeyValue) Lookup(key string) interface{} {
	//stringyKey := fmt.Sprintf("%s", key)
	intKey := createHash(key)
	return kv.data[intKey]
}

func (kv KeyValue) Update(key string, newValue interface{}) {
	intKey := createHash(key)
	kv.data[intKey] = newValue
}

func (kv KeyValue) Delete(key uint32) {
	delete(kv.data, key)
}
