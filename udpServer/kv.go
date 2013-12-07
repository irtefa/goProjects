package main

type KVData struct {
	Command string      `json:"Command"`
	Origin  string      `json:"Origin"`
	Key     string      `json:"Key"`
	Value   interface{} `json:"Value"`
}

type KeyValue struct {
	data map[string]interface{}
}

func (kv KeyValue) Insert(key string, value interface{}) {
	kv.data[key] = value
}

func (kv KeyValue) Lookup(key string) interface{} {
	if kv.data[key] == nil {
		return "Key not found"
	}
	return kv.data[key]
}

func (kv KeyValue) Update(key string, newValue interface{}) {
	kv.data[key] = newValue
}

func (kv KeyValue) Delete(key string) {
	delete(kv.data, key)
}
