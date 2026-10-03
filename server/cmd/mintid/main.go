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

// Command mintid issues a primary client id, sealed with the server's issue
// key as the server seals v2 client ids.
//
//	mintid [-config unitdb.conf] [-contract N] [-service] [-ttl 0] [-v1]
//	mintid [-config unitdb.conf] -from ID [-ttl 0]
//
// The keyring is read as the server reads it: UNITDB_KEYRING, else the
// config's keyring_file, else the single key (UNITDB_ENCRYPTION_KEY, else
// the config's encryption_config.key). Without -contract, the id is of a
// new, random contract. -ttl sets how long the id lasts; 0, the default,
// never expires.
//
// -service marks the id as a trusted service's (uid.AllowService): its
// connections, and the ones it vouches for with unitdb/service, publish and
// subscribe on the contract's topics without topic keys, in a cluster too.
// The server never issues such ids: give them only to servers, never to
// clients or devices.
//
// -from seals ID, a client id of either version that a key of the keyring
// sealed, again as a v2 id with the issue key: the same id, so the same
// contract, permissions, uuid (none for a v1 id) and sessions. Use it to
// move a service off a v1 id, or off a key being retired.
//
// -v1 mints a v1 id, which never expires, for a cluster with nodes older
// than v2 client ids.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	jcr "github.com/DisposaBoy/JsonConfigReader"
	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/keys"
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
	configFile := flags.String("config", "", "the server's config, for its keyring; UNITDB_KEYRING and UNITDB_ENCRYPTION_KEY override it")
	contract := flags.Uint("contract", 0, "the contract; a new, random one if 0")
	service := flags.Bool("service", false, "a trusted service's id, whose connections need no topic keys")
	ttl := flags.Duration("ttl", 0, "how long the id lasts; 0 never expires")
	from := flags.String("from", "", "a client id to seal again as a v2 one, with the same contract, permissions and uuid")
	v1 := flags.Bool("v1", false, "mint a v1 id, for a cluster with nodes older than v2 client ids")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", flags.Args())
	}
	if *contract > 1<<32-1 {
		return fmt.Errorf("-contract %d is more than 32 bits", *contract)
	}
	if *ttl < 0 {
		return fmt.Errorf("-ttl %s is negative", *ttl)
	}
	if *from != "" && (*contract != 0 || *service || *v1) {
		return fmt.Errorf("-from keeps the id's contract and permissions: it takes neither -contract, -service nor -v1")
	}
	if *v1 && *ttl != 0 {
		return fmt.Errorf("a v1 id can't expire: -v1 takes no -ttl")
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
	kr, err := cfg.Keyring()
	if err != nil {
		return err
	}
	set, err := keys.New(kr)
	if err != nil {
		return err
	}

	var id uid.ID
	if *from != "" {
		var claims *uid.Claims
		if id, claims, err = set.OpenClientID([]byte(*from)); err != nil {
			return fmt.Errorf("-from: the id does not open with any key of the keyring")
		}
		if claims != nil && claims.Expired(time.Now().Unix()) {
			return fmt.Errorf("-from: the id has expired")
		}
	} else if id, err = uid.MintClientID(uint32(*contract), *service); err != nil {
		return err
	}

	var text string
	var expires string
	if *v1 {
		text, expires = set.EncodeClientIDV1(id), "never"
	} else {
		issuedAt, expiresAt := keys.Times(time.Now(), *ttl)
		if text, err = set.SealClientIDAt(id, issuedAt, expiresAt); err != nil {
			return err
		}
		expires = "never"
		if expiresAt != 0 {
			expires = time.Unix(int64(expiresAt), 0).UTC().Format(time.RFC3339)
		}
	}
	version, uuid := 2, id.Uuid()
	if *v1 {
		version, uuid = 1, 0 // a v1 id carries no uuid
	}
	_, err = fmt.Fprintf(out, "client id: %s\nversion:   %d\ncontract:  %d\nservice:   %t\nuuid:      %d\nkey id:    %d\nexpires:   %s\n",
		text, version, id.Contract(), id.IsService(), uuid, set.IssueKeyID(), expires)
	return err
}
