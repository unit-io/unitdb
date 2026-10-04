package store

import (
	"errors"
	"hash/fnv"
	"strconv"
)

// The canary (docs/backup-restore.md): a node writes a backup
// run's id into the store just before its checkpoint of the run, both as a
// message (the DB) and as a record (memdb), so that the weekly restore test
// can tell a checkpoint that opens with what it held from one that only
// opens. Both are written as records are, sealed at rest when sealing is
// on, so reading them back also proves the escrowed keyring opens them.

const sysCanary = "canary"

// canaryKey is the canary record's memdb key: a hash of a string no other
// record's key comes from.
var canaryKey = func() uint64 {
	h := fnv.New64a()
	h.Write([]byte("\x00unitdb-backup-canary"))
	return h.Sum64()
}()

// canaryTTL keeps the canary message as long as the oldest backup.
const canaryTTL = 400 * 24 * 3600

// WriteCanary writes run's id as the canary message and record.
func WriteCanary(run string) error {
	if err := adp.Put(sysContract, sysTopic(sysCanary, "run"), []byte(run), strconv.Itoa(canaryTTL)); err != nil {
		return err
	}
	return adp.PutMessage(canaryKey, []byte(run))
}

// ReadCanary returns the run ids of the newest canary message and of the
// canary record.
func ReadCanary() (message, record string, err error) {
	// Run ids sort as their times do: the newest is the largest, whatever
	// order the store returns them in.
	msgs, err := adp.Get(sysContract, sysTopic(sysCanary, "run"), "")
	if err != nil {
		return "", "", err
	}
	for _, m := range msgs {
		if s := string(m); s > message {
			message = s
		}
	}
	b, err := adp.GetMessage(canaryKey)
	if err != nil && message == "" {
		return "", "", errors.New("no canary")
	}
	return message, string(b), nil
}
