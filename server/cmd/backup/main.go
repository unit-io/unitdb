// Command backup is unitdb's backup run (docs/backup-restore.md): it
// checkpoints every node of a cluster, one after another, under one run id,
// and writes the run's manifest into every node's checkpoint.
//
//	CHECKPOINT_TOKEN=... backup -nodes http://unitdb-0.unitdb:7374,http://unitdb-1.unitdb:7374,...
//
// A checkpoint holds a node's writes back for up to about 1.5 s, so nodes go
// one at a time: the cluster pauses one node at once. Each node is tried
// -tries times. A node whose checkpoint fails is named in the manifest, the
// run is marked incomplete, and the command exits 1, after the others: a
// restore needs every node of one run. With -upload, each node then sends
// its checkpoint, with the manifest, to the bucket; a failed upload
// fails the command too. It prints the manifest.
//
// The other commands are an operator's, or the restore test's, with a
// reader's identity (tools.go): fetch, fetch-journal and escrow-keyring.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Manifest lists a backup run's checkpoints, one per node.
type Manifest struct {
	// Run is the run's id: when it began, in UTC.
	Run string `json:"run"`
	// Complete is set if every node's checkpoint was taken.
	Complete bool      `json:"complete"`
	Nodes    []NodeRun `json:"nodes"`
	Finished time.Time `json:"finished"`
}

// NodeRun is one node's part of a run: its checkpoint, as the node
// described it (POST /_checkpoint's answer), or why there is none.
type NodeRun struct {
	URL        string          `json:"url"`
	Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
	Error      string          `json:"error,omitempty"`
	// ManifestError is why the manifest couldn't be kept on the node.
	ManifestError string `json:"manifest_error,omitempty"`
	// Upload is the node's answer to the upload, or UploadError why it
	// failed; neither is in the manifest the nodes keep.
	Upload      json.RawMessage `json:"upload,omitempty"`
	UploadError string          `json:"upload_error,omitempty"`
}

func main() {
	if len(os.Args) > 1 {
		if cmd, ok := tools[os.Args[1]]; ok {
			if err := cmd(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "backup %s: %v\n", os.Args[1], err)
				os.Exit(1)
			}
			return
		}
	}
	nodes := flag.String("nodes", os.Getenv("UNITDB_MONITOR_URLS"), "The nodes' monitor URLs, comma-separated, as http://unitdb-0.unitdb:7374.")
	tries := flag.Int("tries", 3, "Attempts per node.")
	wait := flag.Duration("retry_wait", 10*time.Second, "Time between a node's attempts.")
	timeout := flag.Duration("timeout", 2*time.Minute, "Longest a node's checkpoint may take.")
	upload := flag.Bool("upload", false, "Have each node upload its checkpoint and the manifest to the bucket (BACKUP_S3_* on the nodes).")
	uploadTimeout := flag.Duration("upload_timeout", time.Hour, "Longest a node's upload may take.")
	flag.Parse()
	token := os.Getenv("CHECKPOINT_TOKEN")
	if *nodes == "" || token == "" {
		fmt.Fprintln(os.Stderr, "backup: -nodes (or UNITDB_MONITOR_URLS) and CHECKPOINT_TOKEN are needed")
		os.Exit(2)
	}
	r := runner{token: token, tries: *tries, wait: *wait, client: &http.Client{Timeout: *timeout}}
	m := r.run(time.Now().UTC(), strings.Split(*nodes, ","))
	uploaded := true
	if *upload && m.Complete {
		r.client = &http.Client{Timeout: *uploadTimeout}
		uploaded = r.uploadAll(m)
	}
	out, _ := json.MarshalIndent(m, "", "  ")
	fmt.Println(string(out))
	if !m.Complete || !uploaded {
		os.Exit(1)
	}
}

type runner struct {
	token  string
	tries  int
	wait   time.Duration
	client *http.Client
}

// run takes a backup run that began at start.
func (r *runner) run(start time.Time, urls []string) *Manifest {
	m := &Manifest{Run: start.Format("20060102T150405Z"), Complete: true}
	for _, u := range urls {
		u = strings.TrimRight(strings.TrimSpace(u), "/")
		if u == "" {
			continue
		}
		n := NodeRun{URL: u}
		var err error
		for i := 0; i < r.tries; i++ {
			if i > 0 {
				time.Sleep(r.wait)
			}
			if n.Checkpoint, err = r.checkpoint(u, m.Run); err == nil {
				break
			}
		}
		if err != nil {
			n.Error = err.Error()
			m.Complete = false
			fmt.Fprintf(os.Stderr, "backup: run %s: %s: %v\n", m.Run, u, err)
		}
		m.Nodes = append(m.Nodes, n)
	}
	m.Finished = time.Now().UTC()
	body, _ := json.MarshalIndent(m, "", "  ")
	for i := range m.Nodes {
		n := &m.Nodes[i]
		if n.Error != "" {
			continue
		}
		if err := r.putManifest(n.URL, m.Run, body); err != nil {
			n.ManifestError = err.Error()
			fmt.Fprintf(os.Stderr, "backup: run %s: manifest on %s: %v\n", m.Run, n.URL, err)
		}
	}
	return m
}

// uploadAll has each node upload its checkpoint of the run, one after
// another, each tried r.tries times, and reports whether all did.
func (r *runner) uploadAll(m *Manifest) bool {
	ok := true
	for i := range m.Nodes {
		n := &m.Nodes[i]
		var err error
		for t := 0; t < r.tries; t++ {
			if t > 0 {
				time.Sleep(r.wait)
			}
			req, rerr := http.NewRequest(http.MethodPost, n.URL+"/_checkpoint/upload?run="+url.QueryEscape(m.Run), nil)
			if rerr != nil {
				err = rerr
				break
			}
			var b []byte
			if b, err = r.do(req); err == nil {
				n.Upload = json.RawMessage(b)
				break
			}
		}
		if err != nil {
			n.UploadError = err.Error()
			ok = false
			fmt.Fprintf(os.Stderr, "backup: run %s: upload from %s: %v\n", m.Run, n.URL, err)
		}
	}
	return ok
}

// checkpoint has the node at u take its checkpoint of run, and returns its
// description of it.
func (r *runner) checkpoint(u, run string) (json.RawMessage, error) {
	req, err := http.NewRequest(http.MethodPost, u+"/_checkpoint?run="+url.QueryEscape(run), nil)
	if err != nil {
		return nil, err
	}
	b, err := r.do(req)
	if err != nil {
		return nil, err
	}
	if !json.Valid(b) {
		return nil, fmt.Errorf("the answer isn't JSON: %.200s", b)
	}
	return json.RawMessage(b), nil
}

// putManifest keeps the run's manifest in the node's checkpoint of it.
func (r *runner) putManifest(u, run string, manifest []byte) error {
	req, err := http.NewRequest(http.MethodPut, u+"/_checkpoint/manifest?run="+url.QueryEscape(run), bytes.NewReader(manifest))
	if err != nil {
		return err
	}
	_, err = r.do(req)
	return err
}

func (r *runner) do(req *http.Request) ([]byte, error) {
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return b, nil
}
