package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
)

// The keyring is escrowed apart from the backups, in AWS Secrets Manager:
// a backup restores only with the keyring of its time (records sealed at
// rest, client ids and topic keys). Every change of the keyring is put
// there first, and a key leaves the escrow only when no backup within
// retention names it (the manifests' key_ids): the escrow holds every key,
// those retired from the keyring as "read" ones.

// KeyringSecret is the name of a cluster's escrowed keyring.
func KeyringSecret(cluster string) string {
	return "unitdb/" + cluster + "/keyring"
}

// BackupKeySecret is the name of the backup key's private half.
func BackupKeySecret(cluster string) string {
	return "unitdb/" + cluster + "/backup-key"
}

// SecretStore holds secrets by name.
type SecretStore interface {
	// Get returns the secret's value; found is false if there is none.
	Get(ctx context.Context, name string) (value string, found bool, err error)
	// Put sets the secret's value, making it if there is none.
	Put(ctx context.Context, name, value string) error
}

// keyringKey is a key of the keyring, as UNITDB_KEYRING holds it.
type keyringKey struct {
	ID  int    `json:"id"`
	Key string `json:"key"`
	Use string `json:"use"`
}

// MergeKeyring returns the escrow after current, the keyring in use, is put
// in it: every key of both, by id, with current's use; a key the keyring
// no longer has is kept as a "read" key. The same id with another key is
// an error: a key is never replaced.
func MergeKeyring(escrowed, current string) (string, error) {
	var esc, cur []keyringKey
	if escrowed != "" {
		if err := json.Unmarshal([]byte(escrowed), &esc); err != nil {
			return "", fmt.Errorf("backup: the escrowed keyring: %v", err)
		}
	}
	if err := json.Unmarshal([]byte(current), &cur); err != nil || len(cur) == 0 {
		return "", fmt.Errorf("backup: the keyring: %v", err)
	}
	byID := make(map[int]keyringKey)
	for _, k := range esc {
		k.Use = "read"
		byID[k.ID] = k
	}
	for _, k := range cur {
		if old, ok := byID[k.ID]; ok && old.Key != k.Key {
			return "", fmt.Errorf("backup: key %d differs from the escrowed one: a key id is never reused", k.ID)
		}
		byID[k.ID] = k
	}
	merged := make([]keyringKey, 0, len(byID))
	for _, k := range byID {
		merged = append(merged, k)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].ID < merged[j].ID })
	b, err := json.Marshal(merged)
	return string(b), err
}

// EscrowKeyring puts the keyring in use into the cluster's escrow.
func EscrowKeyring(ctx context.Context, ss SecretStore, cluster, keyring string) error {
	name := KeyringSecret(cluster)
	old, _, err := ss.Get(ctx, name)
	if err != nil {
		return err
	}
	merged, err := MergeKeyring(old, keyring)
	if err != nil {
		return err
	}
	if merged == old {
		return nil
	}
	return ss.Put(ctx, name, merged)
}

// EscrowedKeyIDs returns the ids of the keys in an escrowed keyring.
func EscrowedKeyIDs(escrowed string) (map[int]bool, error) {
	var esc []keyringKey
	if err := json.Unmarshal([]byte(escrowed), &esc); err != nil {
		return nil, err
	}
	ids := make(map[int]bool, len(esc))
	for _, k := range esc {
		ids[k.ID] = true
	}
	return ids, nil
}

// AWSSecrets is AWS Secrets Manager.
type AWSSecrets struct {
	client *secretsmanager.Client
}

// NewAWSSecrets returns Secrets Manager in region (the AWS SDK's if empty),
// with credentials from the AWS SDK's chain.
func NewAWSSecrets(ctx context.Context, region string) (*AWSSecrets, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &AWSSecrets{client: secretsmanager.NewFromConfig(cfg)}, nil
}

func (a *AWSSecrets) Get(ctx context.Context, name string) (string, bool, error) {
	out, err := a.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(name)})
	var nf *smtypes.ResourceNotFoundException
	if errors.As(err, &nf) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return aws.ToString(out.SecretString), true, nil
}

func (a *AWSSecrets) Put(ctx context.Context, name, value string) error {
	_, err := a.client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{SecretId: aws.String(name), SecretString: aws.String(value)})
	var nf *smtypes.ResourceNotFoundException
	if errors.As(err, &nf) {
		_, err = a.client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String(name), SecretString: aws.String(value)})
	}
	return err
}
