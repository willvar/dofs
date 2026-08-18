# DOFS

DOFS turns encrypted S3-compatible object storage into an ordinary,
namespace-scoped Linux filesystem. Applications use POSIX file I/O and receive
neither object-store credentials nor plaintext encryption keys.

```text
application / container
        │ open, read, write, fsync, rename, unlink
        ▼
      DOFS (FUSE)
        ├─ stable inode namespace and immutable generations
        ├─ per-namespace envelope encryption
        ├─ sparse plaintext writeback cache and recovery WAL
        └─ SQLite (default) or PostgreSQL metadata
                         │
                         ▼
                  S3-compatible storage
```

The repository builds and runs independently of Domus. Its contracts are
application-neutral: a tenant is an immutable namespace ID, logical paths are
metadata rather than physical object keys, and each committed file generation
is an immutable encrypted object.

## What DOFS guarantees

- AES-256-GCM chunk encryption with a distinct data key per file and a wrapped
  key per namespace.
- Range reads decrypt only the encrypted chunks covering the requested bytes.
- Create, mkdir, atomic rename, unlink, random write, truncate, flush and
  fsync on Linux FUSE.
- Sparse local writeback state, a durable WAL, immutable uploads and
  generation compare-and-swap publication.
- Portable S3 writes use a cryptographically random generation key, reject an
  already-existing key before upload, and rely on metadata CAS for atomic
  publication; they do not require MinIO's non-standard conditional PUT.
- One writable FUSE owner per namespace, with lease-loss detection.
- Namespace quotas, durable change events and stable inode identity.
- A direct-upload reservation API: a browser or application can upload
  ciphertext straight to object storage and publish it without proxying the
  payload through its control server.

DOFS is a filesystem over objects, not a block device. Keep databases,
package-manager state, Unix sockets and high-churn build caches on local
storage. Symlinks, hard links, special nodes, mmap/database durability and
POSIX advisory locks are outside the current contract.

## Trust and storage boundary

DOFS is server-managed application-layer encryption, not a zero-knowledge
system. A DOFS process can unwrap namespace and file keys in memory so it can
serve plaintext filesystem I/O. An application container should receive only
one exact FUSE bind mount; it should not receive the DOFS master key, S3
credentials, `/dev/fuse`, a control socket or a container-runtime socket.

The writeback cache and recovery WAL can contain plaintext. `state_root` must
be mode `0700` on an encrypted local volume and must not be placed on S3, NFS,
an image layer or an unprotected backup.

## Platform and metadata choices

The library compiles on other Go targets, but mounting is deliberately
Linux-only because the production filesystem uses Linux FUSE. This keeps the
metadata, crypto and object-store code portable while making the actual host
contract explicit.

| Driver | Binary | Use it for | Boundary |
| --- | --- | --- | --- |
| SQLite (default) | `dofsd` | One Linux host, including several namespace mount processes | Database file and WAL must be on one local filesystem; do not share it over NFS. |
| PostgreSQL | `dofsd-postgres` | Several DOFS/control processes or hosts sharing metadata | Host-local plaintext WAL still requires scheduler fencing before automatic failover. |

SQLite is a real production default for the single-host shape, not a test-only
fallback. It uses WAL mode, full synchronous durability, foreign keys,
immediate write transactions and a durable heartbeat lease. PostgreSQL is an
optional distribution so the default binary does not carry a PostgreSQL
driver or require a PostgreSQL server.

## Build and test

Go 1.26.1 or newer is required by the current module.

```bash
make build                 # bin/dofsd, SQLite
make build-postgres        # bin/dofsd-postgres
make test
make test-postgres
make vet
```

Real FUSE, Docker handoff and SQLite + S3 + restart contracts are opt-in:

```bash
make test-fuse
make test-docker
make test-e2e
```

Those targets require `/dev/fuse`; Docker/S3 targets also require the local
services described in the test source and environment variables.

## Configure

Start from [`config.example.yaml`](config.example.yaml). Secrets can come only
from an environment variable or a mounted file; inline secret values are not
part of the schema.

```bash
install -d -m 0700 /var/lib/dofs
install -d -m 0711 /mnt/dofs
install -m 0600 config.example.yaml /etc/dofs/config.yaml
openssl rand -hex 32       # set once as DOFS_MASTER_KEY
```

The master key is part of the data recovery boundary. Losing or changing it
makes wrapped namespace keys unusable. Back it up separately from metadata and
encrypted objects.

Initialize metadata, create a namespace and verify all configured resources:

```bash
bin/dofsd init --config /etc/dofs/config.yaml
bin/dofsd namespace create --config /etc/dofs/config.yaml \
  --id marvo --label Marvo --quota 100GiB
bin/dofsd check --config /etc/dofs/config.yaml
```

Mount in the foreground:

```bash
bin/dofsd mount --config /etc/dofs/config.yaml --namespace marvo
```

The default mountpoint is `/mnt/dofs/marvo`. Send `SIGINT` or `SIGTERM` for a
normal unmount. `--read-only`, `--writable`, `--mountpoint` and `--debug`
override mount behavior where applicable.

When a different host UID or a container consumes the mount, set
`mount.allow_other: true`, configure the presented `uid`/`gid`, and add an
uncommented `user_allow_other` line to `/etc/fuse.conf`. DOFS validates these
requirements before mounting.

## systemd deployment

The supplied files are templates rather than an installer:

```text
deploy/systemd/dofs.sysusers.conf  -> /usr/lib/sysusers.d/dofs.conf
deploy/systemd/dofs.tmpfiles.conf  -> /usr/lib/tmpfiles.d/dofs.conf
deploy/systemd/dofs-init.service   -> /etc/systemd/system/dofs-init.service
deploy/systemd/dofsd@.service      -> /etc/systemd/system/dofsd@.service
config.example.yaml                -> /etc/dofs/config.yaml
deploy/systemd/dofs.env.example    -> /etc/dofs/dofs.env (optional)
```

Then run:

```bash
systemd-sysusers
systemd-tmpfiles --create /usr/lib/tmpfiles.d/dofs.conf
systemctl daemon-reload
systemctl enable --now dofs-init.service dofsd@marvo.service
```

The template uses `Type=notify`: it becomes ready only after the kernel FUSE
mount handshake and object-store preflight succeed. It intentionally remains
in the host mount namespace so Docker and host services can see the mount.
Do not add `PrivateTmp`, `ProtectSystem`, `ProtectHome`, `RootDirectory`,
`BindPaths`, `InaccessiblePaths` or similar filesystem-namespace directives to
`dofsd@.service`. `NoNewPrivileges` is also omitted because an unprivileged
service commonly needs the setuid `fusermount3` helper.

For a consuming service, declare both `Requires=dofsd@marvo.service` and
`After=dofsd@marvo.service`, then use `/mnt/dofs/marvo` like a normal
filesystem. Bind only that exact child into a container, with `rprivate`
propagation; never bind the shared mount root.

## Application integration

The lowest-cost integration is ordinary filesystem I/O. A service such as
Marvo can depend on the namespace mount and keep its existing `os.Open`,
`os.WriteFile`, rename and directory operations.

Applications that also have a browser upload path can use DOFS as a library:

1. Create a control-plane `Backend` for one namespace.
2. Call `PrepareDirectUpload` with the plaintext size and file key.
3. Presign only the returned immutable object key.
4. Encrypt in the browser/application and upload ciphertext directly to S3.
5. Call `CommitDirectUpload`; DOFS validates encrypted geometry/header and
   atomically publishes the reserved generation.
6. Durably update application projections, then call
   `AcknowledgeDirectUpload`.

The payload never needs to traverse the application server. Metadata
transactions and generation CAS safely arbitrate races with a writable FUSE
mount. The caller still owns authentication, S3 presigning, CORS policy and
the decision of how a client receives or creates the file key.

## Operations and recovery

Useful administrative commands are:

```bash
bin/dofsd namespace list --config /etc/dofs/config.yaml --json
bin/dofsd namespace show --config /etc/dofs/config.yaml --id marvo --json
bin/dofsd namespace delete --config /etc/dofs/config.yaml --id marvo --yes
bin/dofsd check --config /etc/dofs/config.yaml
```

Namespace deletion is permanent. It first removes the metadata catalog under
the writer fence, then reclaims only that namespace's exact encrypted-object
prefix. The object cleanup is retryable if interrupted.

A prepared writeback WAL is replayed on the next writable mount. Uncommitted
dirty state is discarded. A generation CAS loser cannot publish over a newer
version. Superseded generations are retained while old readers may still be
using them; inode deletion reclaims all generations. A separate retention/GC
policy for long-lived overwritten files is not included yet.

## Current deployment boundary

- Linux FUSE and a host-visible mount namespace are required.
- SQLite supports one host; PostgreSQL coordinates shared metadata but does
  not by itself make host-local writeback state safe for automatic failover.
- Containers share the host kernel. Hostile multi-tenant execution needs an
  additional sandbox or VM boundary.
- Object-store CORS, lifecycle/versioning, least-privilege credentials,
  backups and master-key custody remain deployment responsibilities.
- The project is pre-1.0. Pin an explicit v0.x release or a reviewed commit.

## License

DOFS is licensed under the [Apache License 2.0](LICENSE).
