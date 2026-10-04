// Package backup keeps unitdb's backups off the cluster
// (docs/backup-restore.md): each node's checkpoint of a run,
// archived (tar + zstd) and encrypted to the backup key's public half (age),
// and the security journal, in an S3 bucket with Object Lock. The cluster
// writes with an identity that may only put objects: it can neither read
// nor erase its backups. Reading them (fetch, the restore test) needs the
// backup key's private half and a reader's identity, kept apart.
//
// The bucket's layout, under one prefix per cluster:
//
//	<cluster>/<run>/manifest.json
//	<cluster>/<run>/<node>.tar.zst.age
//	<cluster>/journal/<date>/<node>-<time>.jsonl
package backup

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Config is where a cluster's backups go, from the environment:
//
//	BACKUP_S3_BUCKET     the bucket; uploads are off without it
//	BACKUP_S3_REGION     the region; the AWS SDK's (AWS_REGION, a profile) if unset
//	BACKUP_S3_ENDPOINT   another S3 endpoint (MinIO), path-style; AWS if unset
//	BACKUP_CLUSTER       the cluster's prefix in the bucket, e.g. production
//	BACKUP_AGE_RECIPIENT the backup key's public half (age1...)
//
// Credentials come from the AWS SDK's chain: AWS_ACCESS_KEY_ID and
// AWS_SECRET_ACCESS_KEY from a Secret, or a profile.
type Config struct {
	Bucket    string
	Region    string
	Endpoint  string
	Cluster   string
	Recipient string
}

// ConfigFromEnv returns the configuration the environment sets, or nil if
// there is no bucket.
func ConfigFromEnv() (*Config, error) {
	c := &Config{
		Bucket:    os.Getenv("BACKUP_S3_BUCKET"),
		Region:    os.Getenv("BACKUP_S3_REGION"),
		Endpoint:  os.Getenv("BACKUP_S3_ENDPOINT"),
		Cluster:   os.Getenv("BACKUP_CLUSTER"),
		Recipient: os.Getenv("BACKUP_AGE_RECIPIENT"),
	}
	if c.Bucket == "" {
		return nil, nil
	}
	if c.Cluster == "" || strings.ContainsAny(c.Cluster, "/ ") {
		return nil, fmt.Errorf("backup: BACKUP_CLUSTER names the cluster's prefix in the bucket, without / or spaces")
	}
	return c, nil
}

// RunPrefix is where a run's objects are.
func (c *Config) RunPrefix(run string) string {
	return c.Cluster + "/" + run + "/"
}

// CheckpointKey is the object of a node's checkpoint of a run.
func (c *Config) CheckpointKey(run, node string) string {
	return c.RunPrefix(run) + node + ".tar.zst.age"
}

// ManifestKey is the object of a run's manifest.
func (c *Config) ManifestKey(run string) string {
	return c.RunPrefix(run) + "manifest.json"
}

// JournalPrefix is where the security journal is.
func (c *Config) JournalPrefix() string {
	return c.Cluster + "/journal/"
}

// JournalKey is the object of one batch of a node's journal, uploaded at t.
func (c *Config) JournalKey(node string, t time.Time) string {
	t = t.UTC()
	return fmt.Sprintf("%s%s/%s-%s.jsonl", c.JournalPrefix(), t.Format("2006-01-02"), node, t.Format("20060102T150405.000000000Z"))
}

// Retention is how long an object can't be deleted or overwritten, and the
// tag the bucket's lifecycle rules expire it by.
type Retention struct {
	Tier  string // daily, weekly, monthly or journal
	Until time.Time
}

// Retentions kept: 7 daily, 4 weekly and 12 monthly runs, and the journal
// as long as the oldest run. Each object is locked a day longer than it is
// kept, and the lifecycle rules expire it after (deploy/aws/README.md).
const (
	dailyKeep   = 7 * 24 * time.Hour
	weeklyKeep  = 28 * 24 * time.Hour
	monthlyKeep = 395 * 24 * time.Hour // 13 months
)

// RunRetention returns the retention of a run that began at t: the first
// run of a month is monthly, of a week (Sunday) weekly, any other daily.
func RunRetention(t time.Time) Retention {
	t = t.UTC()
	switch {
	case t.Day() == 1:
		return Retention{Tier: "monthly", Until: t.Add(monthlyKeep + 24*time.Hour)}
	case t.Weekday() == time.Sunday:
		return Retention{Tier: "weekly", Until: t.Add(weeklyKeep + 24*time.Hour)}
	}
	return Retention{Tier: "daily", Until: t.Add(dailyKeep + 24*time.Hour)}
}

// JournalRetention is the retention of a journal batch uploaded at t: as
// long as a monthly run, since a restore of the oldest run replays it.
func JournalRetention(t time.Time) Retention {
	return Retention{Tier: "journal", Until: t.UTC().Add(monthlyKeep + 24*time.Hour)}
}
