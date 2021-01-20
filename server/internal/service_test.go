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
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	jcr "github.com/DisposaBoy/JsonConfigReader"
	"github.com/stretchr/testify/assert"
	"github.com/unit-io/unitdb/server/internal/config"
	lp "github.com/unit-io/unitdb/server/internal/net"
)

func TestPubsub(t *testing.T) {
	var cfg *config.Config
	// Get the directory of the process
	// exe, err := os.Executable()
	_, exe, _, _ := runtime.Caller(0)
	configfile := filepath.Join(filepath.Dir(exe), "../unitdb.conf")
	if file, err := os.Open(configfile); err != nil {
		assert.NoError(t, err)
	} else if err = json.NewDecoder(jcr.New(file)).Decode(&cfg); err != nil {
		assert.NoError(t, err)
	}
	svc, err := NewService(context.Background(), cfg)
	assert.NoError(t, err)

	defer svc.Close()

	go svc.Listen()

	// Create a client
	cli, err := net.Dial("tcp", "127.0.0.1:6060")
	assert.NoError(t, err)
	defer cli.Close()

	{ // Connect to the broker
		connect := lp.Connect{ClientID: []byte("UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ")}
		n := connect.Encode()
		assert.Equal(t, 14, n)
		assert.NoError(t, err)
	}

	{ // Read connack
		msg, err := lp.ReadPacket(cli)
		assert.NoError(t, err)
		assert.Equal(t, lp.CONNACK, msg.Type())
	}

	{ // Ping the broker
		ping := lp.Pingreq{}
		n := ping.Encode()
		assert.Equal(t, 2, n)
		assert.NoError(t, err)
	}

	{ // Read pong
		msg, err := lp.ReadPacket(cli)
		assert.NoError(t, err)
		assert.Equal(t, lp.PINGRESP, msg.Type())
	}

	{ // Subscribe to a topic
		sub := lp.Subscribe{
			FixedHeader: lp.FixedHeader{Qos: 0},
			Subscriptions: []lp.TopicQOSTuple{
				{Topic: []byte("AYAAMACRZDCHK/..."), Qos: 0},
			},
		}
		sub.Encode()
		assert.NoError(t, err)
	}

	{ // Read suback
		msg, err := lp.ReadPacket(cli)
		assert.NoError(t, err)
		assert.Equal(t, lp.SUBACK, msg.Type())
	}

	{ // Publish a message
		msg := lp.Publish{
			FixedHeader: lp.FixedHeader{Qos: 0},
			Topic:       []byte("AbYANcEEZDcdY/unit8.b.b1?ttl=3m"),
			Payload:     []byte("Hi unit8.b.b1!"),
		}
		msg.Encode()
		assert.NoError(t, err)
	}

	{ // Read the message back
		msg, err := lp.ReadPacket(cli)
		assert.NoError(t, err)
		assert.Equal(t, lp.PUBLISH, msg.Type())
		assert.Equal(t, &lp.Publish{
			FixedHeader: lp.FixedHeader{Qos: 0},
			Topic:       []byte("unit8.b.b1"),
			Payload:     []byte("Hi unit8.b.b1!"),
		}, msg)
	}

	{ // Unsubscribe from the topic
		sub := lp.Unsubscribe{
			FixedHeader: lp.FixedHeader{Qos: 0},
			Topics: []lp.TopicQOSTuple{
				{Topic: []byte("AYAAMACRZDCHK/..."), Qos: 0},
			},
		}
		sub.Encode()
		assert.NoError(t, err)
	}

	{ // Read unsuback
		msg, err := lp.ReadPacket(cli)
		assert.NoError(t, err)
		assert.Equal(t, lp.UNSUBACK, msg.Type())
	}

	{ // Disconnect from the broker
		disconnect := lp.Disconnect{}
		n := disconnect.Encode()
		assert.Equal(t, 2, n)
		assert.NoError(t, err)
	}

}
