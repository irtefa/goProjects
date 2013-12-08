package main

type KVData struct {
	Command string      `json:"Command"`
	Origin  string      `json:"Origin"`
	Key     string      `json:"Key"`
	Value   interface{} `json:"Value"`
	Version float64     `json:"Version"`
}

type KeyValue struct {
	data    map[string]interface{}
	version map[string]float64
}

func (kv KeyValue) Insert(key string, value interface{}) {
	kv.data[key] = value
	kv.version[key] += 1
}

func (kv KeyValue) Lookup(key string) interface{} {
	if kv.data[key] == nil {
		return "Key not found"
	}
	kv.version[key] += 1
	return kv.data[key]
}

func (kv KeyValue) Update(key string, newValue interface{}) {
	kv.data[key] = newValue
	kv.version[key] += 1
}

func (kv KeyValue) Delete(key string) {
	delete(kv.data, key)
}

func (kv KeyValue) GetVersion(key string) float64 {
	return kv.version[key]
}
