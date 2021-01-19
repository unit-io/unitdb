package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"time"

	jcr "github.com/DisposaBoy/JsonConfigReader"
	"github.com/rs/zerolog"
	"github.com/unit-io/unitdb/server/config"
	"github.com/unit-io/unitdb/server/pkg/log"
)

func main() {
	// Get the directory of the process
	exe, err := os.Executable()
	if err != nil {
		panic(err.Error())
	}

	var configfile = flag.String("config", "unitdb.conf", "Path to config file.")
	var listenOn = flag.String("listen", "", "Override address and port to listen on for HTTP(S) clients.")
	var clusterSelf = flag.String("cluster_self", "", "Override the name of the current cluster node")
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

	if *varzPath != "" {
		cfg.VarzPath = *varzPath
	}

	// Initialize cluster and receive calculated workerId.
	// Cluster won't be started here yet.
	ClusterInit(cfg.Cluster, clusterSelf)

	Globals.ConnCache = NewConnCache()

	svc, err := NewService(context.Background(), cfg)
	if err != nil {
		panic(err.Error())
	}

	// Start accepting cluster traffic.
	if Globals.Cluster != nil {
		Globals.Cluster.Start()
	}

	Globals.Service = svc

	//Listen and serve
	svc.Listen()
	log.Info("main", "Service is running at port "+cfg.Listen)
}
