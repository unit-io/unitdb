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

// Command mintid issues a primary client id, encrypted with the server's
// encryption key as the server encrypts client ids.
//
//	mintid [-config unitdb.conf] [-contract N] [-service]
//
// The key is read as the server reads it: UNITDB_ENCRYPTION_KEY, else the
// config's encryption_config.key. Without -contract, the id is of a new,
// random contract.
//
// -service marks the id as a trusted service's (uid.AllowService): its
// connections, and the ones it vouches for with unitdb/service, publish and
// subscribe on the contract's topics without topic keys, in a cluster too.
// The server never issues such ids: give them only to servers, never to
// clients or devices.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	jcr "github.com/DisposaBoy/JsonConfigReader"
	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mintid:", err)
		os.Exit(1)
	}
}

// run mints a client id as args say, and writes it to out.
func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("mintid", flag.ContinueOnError)
	configFile := flags.String("config", "", "the server's config, for its encryption key; UNITDB_ENCRYPTION_KEY overrides it")
	contract := flags.Uint("contract", 0, "the contract; a new, random one if 0")
	service := flags.Bool("service", false, "a trusted service's id, whose connections need no topic keys")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", flags.Args())
	}
	if *contract > 1<<32-1 {
		return fmt.Errorf("-contract %d is more than 32 bits", *contract)
	}

	var cfg config.Config
	if *configFile != "" {
		f, err := os.Open(*configFile)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := json.NewDecoder(jcr.New(f)).Decode(&cfg); err != nil {
			return fmt.Errorf("%s: %v", *configFile, err)
		}
	}
	key, err := cfg.EncryptionKey()
	if err != nil {
		return err
	}
	mac, err := crypto.New(key)
	if err != nil {
		return err
	}
	id, err := uid.MintClientID(uint32(*contract), *service)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "client id: %s\ncontract:  %d\nservice:   %t\n", id.Encode(mac), id.Contract(), id.IsService())
	return err
}
