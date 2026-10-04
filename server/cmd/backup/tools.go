package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"filippo.io/age"

	"github.com/unit-io/unitdb/server/internal/backup"
)

// The operator's commands, run with a reader's identity (AWS credentials
// allowed to read the bucket, and the backup key):
//
//	backup fetch -run <id> -out <dir> [-node <node>]
//	    downloads a run, decrypts each node's checkpoint into <dir>/<node>,
//	    or one node's into <dir>, to start each node at with -restored
//	    (docs/backup-restore.md, runbook B).
//	backup runs
//	    lists the runs in the bucket, newest first, and whether each is
//	    complete: a restore takes the newest complete one.
//	backup fetch-journal -since <run id> -out <dir>
//	    downloads the security journal from the run's day on, for -journal.
//	backup escrow-keyring -keyring <file>
//	    puts the keyring in use into the cluster's escrow in Secrets
//	    Manager, before it goes into the cluster's Secret.
//
// The bucket and cluster come from BACKUP_S3_*, BACKUP_CLUSTER. The backup
// key's private half is read from -identity (a file age-keygen wrote),
// BACKUP_AGE_IDENTITY, or else Secrets Manager.
var tools = map[string]func(args []string) error{
	"fetch":          fetch,
	"fetch-journal":  fetchJournal,
	"escrow-keyring": escrowKeyring,
	"verify":         verify,
	"runs":           runs,
}

func storeFromEnv(ctx context.Context) (*backup.Store, error) {
	cfg, err := backup.ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, errors.New("BACKUP_S3_BUCKET and BACKUP_CLUSTER name the bucket")
	}
	return backup.NewStore(ctx, cfg)
}

// identity returns the backup key's private half.
func identity(ctx context.Context, file string, cfg *backup.Config) (age.Identity, error) {
	switch {
	case file != "":
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		return backup.ParseIdentity(string(b))
	case os.Getenv("BACKUP_AGE_IDENTITY") != "":
		return backup.ParseIdentity(os.Getenv("BACKUP_AGE_IDENTITY"))
	}
	ss, err := backup.NewAWSSecrets(ctx, cfg.Region)
	if err != nil {
		return nil, err
	}
	v, found, err := ss.Get(ctx, backup.BackupKeySecret(cfg.Cluster))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("no %s in Secrets Manager", backup.BackupKeySecret(cfg.Cluster))
	}
	return backup.ParseIdentity(v)
}

func fetch(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	run := fs.String("run", "", "The run's id, as 20060102T150405Z.")
	out := fs.String("out", "", "Where to put each node's checkpoint: <out>/<node>; with -node, <out> itself.")
	node := fs.String("node", "", "Only this node's checkpoint, into <out>: a restore Job's, on the node's new claim.")
	idFile := fs.String("identity", "", "The backup key's private half, as age-keygen wrote it.")
	fs.Parse(args)
	if *run == "" || *out == "" {
		return errors.New("-run and -out are needed")
	}
	ctx := context.Background()
	st, err := storeFromEnv(ctx)
	if err != nil {
		return err
	}
	id, err := identity(ctx, *idFile, st.Config)
	if err != nil {
		return err
	}
	if *node != "" {
		m, err := readManifest(ctx, st, *run)
		if err != nil {
			return err
		}
		if !m.Complete {
			return fmt.Errorf("run %s is incomplete: restore every node from one complete run", *run)
		}
		found := false
		for _, n := range m.Nodes {
			found = found || n.Checkpoint.Node == *node
		}
		if !found {
			return fmt.Errorf("run %s has no checkpoint of %s", *run, *node)
		}
		obj, err := st.Get(ctx, st.Config.CheckpointKey(*run, *node))
		if err != nil {
			return fmt.Errorf("%s's checkpoint: %v", *node, err)
		}
		defer obj.Close()
		if err := backup.ExtractArchive(obj, id, *out); err != nil {
			return fmt.Errorf("%s's checkpoint: %v", *node, err)
		}
		fmt.Println(*out)
		return nil
	}
	_, dirs, err := fetchRun(ctx, st, id, *run, *out)
	for _, d := range dirs {
		fmt.Println(d)
	}
	return err
}

// runManifest is what fetch and verify read of a run's manifest.
type runManifest struct {
	Run      string `json:"run"`
	Complete bool   `json:"complete"`
	Nodes    []struct {
		Checkpoint struct {
			Node   string `json:"node"`
			Time   string `json:"time"`
			KeyIDs []int  `json:"key_ids"`
			Stats  struct {
				Messages   int64 `json:"messages"`
				MemEntries int64 `json:"mem_entries"`
			} `json:"stats"`
		} `json:"checkpoint"`
		Error string `json:"error"`
	} `json:"nodes"`
}

func readManifest(ctx context.Context, st *backup.Store, run string) (*runManifest, error) {
	body, err := st.Get(ctx, st.Config.ManifestKey(run))
	if err != nil {
		return nil, fmt.Errorf("run %s's manifest: %v", run, err)
	}
	defer body.Close()
	var m runManifest
	if err := json.NewDecoder(body).Decode(&m); err != nil {
		return nil, fmt.Errorf("run %s's manifest: %v", run, err)
	}
	return &m, nil
}

// fetchRun downloads a complete run, and decrypts each node's checkpoint
// into out/<node>. It returns the manifest and the directories.
func fetchRun(ctx context.Context, st *backup.Store, id age.Identity, run, out string) (*runManifest, []string, error) {
	m, err := readManifest(ctx, st, run)
	if err != nil {
		return nil, nil, err
	}
	if !m.Complete {
		return m, nil, fmt.Errorf("run %s is incomplete: restore every node from one complete run", run)
	}
	var dirs []string
	for _, n := range m.Nodes {
		node := n.Checkpoint.Node
		if node == "" {
			node = "server"
		}
		obj, err := st.Get(ctx, st.Config.CheckpointKey(run, node))
		if err != nil {
			return m, dirs, fmt.Errorf("%s's checkpoint: %v", node, err)
		}
		dst := filepath.Join(out, node)
		err = backup.ExtractArchive(obj, id, dst)
		obj.Close()
		if err != nil {
			return m, dirs, fmt.Errorf("%s's checkpoint: %v", node, err)
		}
		dirs = append(dirs, dst)
	}
	return m, dirs, nil
}

func fetchJournal(args []string) error {
	fs := flag.NewFlagSet("fetch-journal", flag.ExitOnError)
	since := fs.String("since", "", "The run restored from (its id), or a date (2006-01-02): the journal from that day on.")
	out := fs.String("out", "", "Where to put the journal's files.")
	fs.Parse(args)
	if *since == "" || *out == "" {
		return errors.New("-since and -out are needed")
	}
	from, err := time.Parse("20060102T150405Z", *since)
	if err != nil {
		if from, err = time.Parse("2006-01-02", *since); err != nil {
			return errors.New("-since is a run id or a date")
		}
	}
	// A day early: a node's clock, or a batch uploaded after midnight.
	day := from.UTC().Add(-24 * time.Hour).Format("2006-01-02")
	ctx := context.Background()
	st, err := storeFromEnv(ctx)
	if err != nil {
		return err
	}
	keys, err := st.List(ctx, st.Config.JournalPrefix())
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0700); err != nil {
		return err
	}
	n := 0
	for _, key := range keys {
		rest := strings.TrimPrefix(key, st.Config.JournalPrefix())
		if date, _, _ := strings.Cut(rest, "/"); date < day {
			continue
		}
		obj, err := st.Get(ctx, key)
		if err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(*out, path.Base(key)))
		if err == nil {
			_, err = io.Copy(f, obj)
			if cerr := f.Close(); err == nil {
				err = cerr
			}
		}
		obj.Close()
		if err != nil {
			return err
		}
		n++
	}
	fmt.Printf("%d journal files in %s\n", n, *out)
	return nil
}

func escrowKeyring(args []string) error {
	fs := flag.NewFlagSet("escrow-keyring", flag.ExitOnError)
	file := fs.String("keyring", "", "The keyring, as UNITDB_KEYRING holds it; UNITDB_KEYRING if unset.")
	fs.Parse(args)
	keyring := os.Getenv("UNITDB_KEYRING")
	if *file != "" {
		b, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		keyring = string(b)
	}
	if keyring == "" {
		return errors.New("-keyring or UNITDB_KEYRING is needed")
	}
	cluster := os.Getenv("BACKUP_CLUSTER")
	if cluster == "" {
		return errors.New("BACKUP_CLUSTER names the cluster")
	}
	region := os.Getenv("BACKUP_S3_REGION")
	ctx := context.Background()
	ss, err := backup.NewAWSSecrets(ctx, region)
	if err != nil {
		return err
	}
	if err := backup.EscrowKeyring(ctx, ss, cluster, keyring); err != nil {
		return err
	}
	fmt.Printf("escrowed in %s\n", backup.KeyringSecret(cluster))
	return nil
}

func runs(args []string) error {
	ctx := context.Background()
	st, err := storeFromEnv(ctx)
	if err != nil {
		return err
	}
	keys, err := st.List(ctx, st.Config.Cluster+"/")
	if err != nil {
		return err
	}
	var ids []string
	for _, k := range keys {
		parts := strings.Split(strings.TrimPrefix(k, st.Config.Cluster+"/"), "/")
		if len(parts) == 2 && parts[1] == "manifest.json" && runDir.MatchString(parts[0]) {
			ids = append(ids, parts[0])
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(ids)))
	for _, id := range ids {
		status := "complete"
		if m, err := readManifest(ctx, st, id); err != nil {
			status = "unreadable: " + err.Error()
		} else if !m.Complete {
			status = "incomplete"
		}
		fmt.Printf("%s\t%s\n", id, status)
	}
	return nil
}
