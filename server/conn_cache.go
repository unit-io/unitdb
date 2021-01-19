package main

import (
	"sync"

	"github.com/unit-io/unitdb/server/pkg/uid"
)

type ConnCache struct {
	sync.RWMutex
	m map[uid.LID]*Conn
}

func NewConnCache() *ConnCache {
	cache := &ConnCache{
		m: make(map[uid.LID]*Conn),
	}

	return cache
}

func (cc *ConnCache) Add(conn *Conn) {
	cc.Lock()
	defer cc.Unlock()
	cc.m[conn.connid] = conn
}

// Get fetches a connection from cache by connection ID.
func (cc *ConnCache) Get(connid uid.LID) *Conn {
	cc.Lock()
	defer cc.Unlock()
	if conn := cc.m[connid]; conn != nil {
		return conn
	}

	return nil
}

func (cc *ConnCache) Delete(connid uid.LID) {
	cc.Lock()
	defer cc.Unlock()
	delete(cc.m, connid)
}
