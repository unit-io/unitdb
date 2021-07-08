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

package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"time"

	jcr "github.com/DisposaBoy/JsonConfigReader"
	"github.com/rs/zerolog"
	"github.com/unit-io/unitdb/server/internal"
	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

func main() {
	// Get the directory of the process
	exe, err := os.Executable()
	if err != nil {
		panic(err.Error())
	}

	var configfile = flag.String("config", "unitdb.conf", "Path to config file.")
	var listenOn = flag.String("listen", "", "Override address and port to listen on for HTTP(S) clients.")
	var listenGrpcOn = flag.String("grpc_listen", "", "Override address and port to listen on for GRPC clients.")
	var clusterSelf = flag.String("cluster_self", "", "Override the name of the current cluster node")
	var dbPath = flag.String("db_path", "/tmp/unitdb", "Override the db path.")
	var varzPath = flag.String("varz", "/varz", "Expose runtime stats at the given endpoint, e.g. /varz. Disabled if not set")
	flag.Parse()

	// Default level for is fatal, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	//*configfile = toAbsolutePath(rootpath, *configfile)
	*configfile = filepath.Join(filepath.Dir(exe), *configfile)
	log.Debug("main", "Using config from "+*configfile)
	var cfg *config.Config
	if file, err := os.Open(*configfile); err != nil {
		log.Fatal("main", "Failed to read config file", err)
	} else if err = json.NewDecoder(jcr.New(file)).Decode(&cfg); err != nil {
		log.Fatal("main", "Failed to parse config file", err)
	}

	zerolog.DurationFieldUnit = time.Nanosecond
	if cfg.LoggingLevel != "" {
		l := log.ParseLevel(cfg.LoggingLevel, zerolog.InfoLevel)
		zerolog.SetGlobalLevel(l)
	}

	if *listenOn != "" {
		cfg.Listen = *listenOn
	}

	// Set up gRPC server, if one is configured
	if *listenGrpcOn != "" {
		cfg.GrpcListen = *listenGrpcOn
	}

	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}

	if *varzPath != "" {
		cfg.VarzPath = *varzPath
	}

	// Initialize cluster and receive calculated workerId.
	// Cluster won't be started here yet.
	internal.ClusterInit(cfg.Cluster, clusterSelf)

	svc, err := internal.NewService(cfg)
	if err != nil {
		panic(err.Error())
	}

	// Start accepting cluster traffic.
	if internal.Globals.Cluster != nil {
		internal.Globals.Cluster.Start()
	}

	internal.Globals.Service = svc

	// Listen and serve
	svc.Listen()
	log.Info("main", "Service is running at port "+cfg.Listen)
}
