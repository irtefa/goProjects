package main

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

func (kv KeyValue) Update(key uint32, newValue interface{}) {
	kv.data[key] = newValue
}

func (kv KeyValue) Delete(key uint32) {
	delete(kv.data, key)
}
