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
//	mintid [-config unitdb.conf] [-contract N] [-service] [-ttl 0]
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
// -from seals ID, a client id that a key of the keyring sealed, again as a
// v2 id with the issue key: the same id, so the same contract, permissions,
// uuid and sessions. Use it to move a service off a key being retired, or off
// a v1 id: the server refuses v1 ids since v0.7.0, and -from is the only
// place one is still read. A v1 id has no uuid, nor does the v2 id sealed
// from it; it is opened with each key of the keyring, as v1 ids name none.
//
// mintid issues v2 ids only.
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
	from := flags.String("from", "", "a client id, v2 or v1, to seal again as a v2 one, with the same contract, permissions and uuid")
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
	if *from != "" && (*contract != 0 || *service) {
		return fmt.Errorf("-from keeps the id's contract and permissions: it takes neither -contract nor -service")
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
	switch {
	case *from == "":
		if id, err = uid.MintClientID(uint32(*contract), *service); err != nil {
			return err
		}
	case len(*from) == uid.EncodedLenV1:
		// A v1 id names no key and carries no expiry.
		if id, err = keys.OpenV1ClientID(kr, []byte(*from)); err != nil {
			return fmt.Errorf("-from: the v1 id does not open with any key of the keyring")
		}
	default:
		var claims uid.Claims
		if id, claims, err = set.OpenClientID([]byte(*from)); err != nil {
			return fmt.Errorf("-from: the id does not open with any key of the keyring")
		}
		if claims.Expired(time.Now().Unix()) {
			return fmt.Errorf("-from: the id has expired")
		}
	}

	issuedAt, expiresAt := keys.Times(time.Now(), *ttl)
	text, err := set.SealClientIDAt(id, issuedAt, expiresAt)
	if err != nil {
		return err
	}
	expires := "never"
	if expiresAt != 0 {
		expires = time.Unix(int64(expiresAt), 0).UTC().Format(time.RFC3339)
	}
	_, err = fmt.Fprintf(out, "client id: %s\nversion:   %d\ncontract:  %d\nservice:   %t\nuuid:      %d\nkey id:    %d\nexpires:   %s\n",
		text, 2, id.Contract(), id.IsService(), id.Uuid(), set.IssueKeyID(), expires)
	return err
}
