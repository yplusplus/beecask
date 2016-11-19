package beecask

import ()

type KDItem struct {
	fileId    uint64
	valuePos  uint32
	valueSize uint32
	flag      uint32
}

type KeyDir struct {
	dict map[string]*KDItem
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		dict: make(map[string]*KDItem, 1024),
	}
}

func (kd *KeyDir) Get(key string) *KDItem {
	item, ok := kd.dict[key]
	if ok {
		// make a copy
		nitem := *item
		return &nitem
	}
	return nil
}

func (kd *KeyDir) Set(key string, item *KDItem) {
	// make a copy
	nitem := *item
	kd.dict[key] = &nitem
}

func (kd *KeyDir) Delete(key string) {
	delete(kd.dict, key)
}

func (kd *KeyDir) Keys() []string {
	keys := make([]string, 0, len(kd.dict))
	for k, v := range kd.dict {
		if v.flag&RECORD_FLAG_BIT_DELETE == 0 {
			keys = append(keys, k)
		}
	}
	return keys
}
