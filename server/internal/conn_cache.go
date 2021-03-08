/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package internal

import (
	"sync"

	"github.com/unit-io/unitdb/server/internal/pkg/uid"
)

type _ConnCache struct {
	sync.RWMutex
	m map[uid.LID]*_Conn
}

func NewConnCache() *_ConnCache {
	cache := &_ConnCache{
		m: make(map[uid.LID]*_Conn),
	}

	return cache
}

func (cc *_ConnCache) add(conn *_Conn) {
	cc.Lock()
	defer cc.Unlock()
	cc.m[conn.connID] = conn
}

// get fetches a connection from cache by connection ID.
func (cc *_ConnCache) get(connID uid.LID) *_Conn {
	cc.Lock()
	defer cc.Unlock()
	if conn := cc.m[connID]; conn != nil {
		return conn
	}

	return nil
}

func (cc *_ConnCache) delete(connID uid.LID) {
	cc.Lock()
	defer cc.Unlock()
	delete(cc.m, connID)
}
