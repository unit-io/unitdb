/*
 * Copyright 2021 Saffat Technologies, Ltd.
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

package utp

import (
	// "bytes"

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb/server/internal/net"
	pbx "github.com/unit-io/unitdb/server/proto"
)

func unpackRelay(data []byte) lp.LineProtocol {
	var rel pbx.Relay
	proto.Unmarshal(data, &rel)
	var reqs []*lp.RelayRequest
	for _, req := range rel.RelayRequests {
		r := &lp.RelayRequest{}
		r.Topic = []byte(req.Topic)
		r.Last = req.Last
		reqs = append(reqs, r)
	}

	return &lp.Relay{
		MessageID:     uint16(rel.MessageID),
		RelayRequests: reqs,
	}
}
