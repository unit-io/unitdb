# unitdb's backups on AWS

What a cluster's off-site backups need on AWS (docs/backup-restore.md),
made once per cluster by an AWS admin: a bucket with Object Lock, a
put-only identity for the cluster (an access key in a Kubernetes Secret
here; any of the AWS SDK's credential sources will do), and the backup key
and the escrowed keyring in Secrets Manager, readable only by the weekly
restore test's identity and named admins. A cluster per AWS account keeps
staging and production apart.

Below, `CLUSTER` is the cluster's name (`staging`, `production`): its
prefix in the bucket and in the secrets' names. `BUCKET` is the bucket,
`REGION` its region, `ACCOUNT` the account id. Replace them in the JSON
files here before using them.

## 1. The bucket: Object Lock, and a lifecycle by tier

Object Lock can only be turned on when the bucket is made. There is no
default retention: each object is locked in compliance mode by the node
that uploads it, for as long as its tier is kept, so that daily runs can
expire after a week while monthly ones are kept 13 months (server/internal/backup:
daily 8 days, weekly 29, monthly and the journal 396). Compliance mode: not
even the account's root user deletes a locked version before its date.

```sh
aws s3api create-bucket --bucket BUCKET --region REGION \
  --create-bucket-configuration LocationConstraint=REGION \
  --object-lock-enabled-for-bucket
aws s3api put-public-access-block --bucket BUCKET \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket BUCKET \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-lifecycle-configuration --bucket BUCKET --lifecycle-configuration file://lifecycle.json
```

The lifecycle rules (`lifecycle.json`) expire each object by its `tier`
tag once its lock has passed; the old versions go a day later.

## 2. The cluster's identity: put only

An IAM user, `unitdb-backup-writer-CLUSTER`, with `writer-policy.json`
only: it puts objects, with their lock and tag, under `CLUSTER/`, and can
neither read, list nor delete them. A compromised cluster can't read or
erase its backups.

```sh
aws iam create-user --user-name unitdb-backup-writer-CLUSTER
aws iam put-user-policy --user-name unitdb-backup-writer-CLUSTER \
  --policy-name unitdb-backup-put-only --policy-document file://writer-policy.json
aws iam create-access-key --user-name unitdb-backup-writer-CLUSTER
kubectl -n unitdb create secret generic unitdb-backup-writer \
  --from-literal=AWS_ACCESS_KEY_ID=<AccessKeyId> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<SecretAccessKey>
```

Check the policy before the first run, with the IAM policy simulator or a
test upload: the nodes' uploads use exactly `s3:PutObject` (multipart
included), `s3:PutObjectRetention`, `s3:PutObjectTagging` and, when an
upload fails, `s3:AbortMultipartUpload`. The e2e tests run against MinIO as
its root user, so they don't check the policy.

Rotate the key by making a second one, updating the Secret, restarting the
nodes, and deleting the first.

## 3. The backup key

Archives are encrypted with [age](https://age-encryption.org) to the backup
key's public half; the private half is only in Secrets Manager.

```sh
age-keygen -o backup-key.txt     # prints the public key: age1...
aws secretsmanager create-secret --region REGION \
  --name unitdb/CLUSTER/backup-key --secret-string file://backup-key.txt
shred -u backup-key.txt
```

The public half goes into the nodes' environment as `BACKUP_AGE_RECIPIENT`
(docs/backup-restore.md lists the nodes' settings).

## 4. The keyring's escrow

A backup restores only with the keyring of its time. Put the keyring in
escrow before every change of the cluster's `UNITDB_KEYRING`, with an
admin's identity (`escrow-policy.json`):

```sh
BACKUP_CLUSTER=CLUSTER backup escrow-keyring -keyring keyring.json
```

It keeps every key it ever held: a key gone from the keyring stays as a
`read` key, and a key id with another key is refused. Only then update
the cluster's Secret.

## 5. Readers

`reader-policy.json` reads the bucket under `CLUSTER/` and the two secrets:
for the weekly restore test's identity and the named admins' roles,
nothing else. A restore (docs/backup-restore.md):

```sh
export BACKUP_S3_BUCKET=BUCKET BACKUP_CLUSTER=CLUSTER
backup fetch -run <run id> -out restore/run          # each node's checkpoint
backup fetch-journal -since <run id> -out restore/journal
# then each node: -db_path=restore/run/<node> -restored -journal=restore/journal
```
