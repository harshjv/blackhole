# Blackhole

A versioned drive for fleets of agents. Every write names the agent that made it. Every save is a version. One agent's work can be undone without touching anyone else's.

Status: early. It runs end to end on one machine: CLI, MCP server, S3 endpoint, Finder mount and a single-process server, with shards on local disk, S3 or R2. See [Known limits](#known-limits) before you trust it with anything.

## Why

Several agents writing into one folder that is not a git repo (notes, reports, datasets, generated output) have no record of who changed what, and no way to take back one agent's changes without losing the others'. Blackhole records the actor on every commit and makes `undo --by <agent>` a forward commit that reverts only that agent's writes, reporting the files someone else also touched as conflicts instead of clobbering them.

## See it

```
./demo/agents.sh          # three agents share a folder, one wrecks it, one undo takes back only that agent
./demo/fleet.sh           # eight workers write through the S3 endpoint, one run goes bad, then undo, fork and a read-only mount
```

Both pause for enter between scenes; pass `--auto` to run straight through. `agents.sh` needs zsh and a release build. `fleet.sh` uses the bucket in `BLACKHOLE_SHARDS` if set and a local store otherwise; pass `--no-mount` off macOS.

## Build

Rust stable (1.99 tested).

```
cargo build --release
export PATH="$PWD/target/release:$PATH"
```

## Try it

```
blackhole init                                  # creates ./.blackhole (or $BLACKHOLE_ROOT)
blackhole drive create notes                    # prints the owner's admin key
export BLACKHOLE_KEY=<admin key>

blackhole put ./docs notes:/docs --intent "import"
blackhole key create notes --principal agent-7 --prefix /docs      # scoped key, prints it
blackhole put a.txt notes:/docs/a.txt --key <agent-7 key> --intent "rewrite a"

blackhole history notes:/docs/a.txt
blackhole log notes --by agent-7
blackhole cat notes:/docs/a.txt --at '#1'
blackhole undo notes --by agent-7 --since 20m
blackhole restore notes:/docs --at '2h ago'
blackhole fork notes notes-agent-9              # one row, zero bytes copied; prints a key for the fork
```

Keys are the identity. A key is bound to one drive, optionally to a subtree (`--prefix`), optionally read-only, optionally expiring (`--ttl 2h`). The parent's keys do not work on a fork. Every key has a public id (`BHK...`) and a secret: the secret is what `--key`, the MCP bearer header and S3's secret access key take; the id is the S3 access key id. `key create --json` prints both.

## Use it from an agent (MCP)

`blackhole-mcp` serves a drive over stdio. The key it starts with is the actor for every call, and each tool also takes an optional `key` so one server can act for several agents.

```
claude mcp add blackhole \
  -e BLACKHOLE_ROOT=/path/to/.blackhole -e BLACKHOLE_KEY=<key> \
  -- /path/to/blackhole-mcp
```

Tools: `drives`, `list`, `read`, `write`, `delete`, `rename`, `history`, `log`, `restore`, `undo`, `fork`, `diff`, `merge`. Every write takes an `intent` string that lands in the commit log next to the actor. Failures come back as tool errors the agent can read and act on.

## Run it as a server

`blackhole-server` serves every door on one port: S3 at `/`, MCP over streamable HTTP at `/mcp` (send `Authorization: Bearer <key>`), a server-sent events change feed at `/feed/<drive>?after=<seq>`, and a key API.

```
blackhole-server --root /srv/.blackhole --addr 0.0.0.0:9900
curl -X POST http://drive:9900/api/keys -H "Authorization: Bearer $ADMIN" \
  -H 'content-type: application/json' -d '{"drive":"corpus","principal":"run-42","prefix":"/","ttl_seconds":28800}'
curl -N http://drive:9900/feed/corpus?after=0 -H "Authorization: Bearer $KEY"
claude mcp add --transport http blackhole http://drive:9900/mcp --header "Authorization: Bearer $KEY"
```

No TLS built in; put it behind a proxy or on a private network. One process, one authority.

## Keep storage bounded

```
blackhole gc corpus --keep-versions 10 --keep 30d
```

Expires closed versions beyond the newest N per file and older than the age, never a version a fork still sees, and never under legal hold. Expired versions stay in history as "content purged"; their manifests and shards are deleted when nothing else references them. The gc is itself a commit and the chain still verifies.

## Use it from a container (S3)

`blackhole-s3` serves every drive as an S3 bucket (path-style) so any AWS SDK, the aws CLI or rclone can use it. The access key id is a key's id and the secret access key is its secret; the key's principal is the actor on every write.

```
blackhole-s3 --root /path/.blackhole --addr 127.0.0.1:9900
AWS_ACCESS_KEY_ID=<key id> AWS_SECRET_ACCESS_KEY=<secret> aws --endpoint-url http://127.0.0.1:9900 s3 sync ./out s3://notes/out
```

Supported: list buckets, list objects (v1 and v2, prefix, delimiter, paginated with max-keys and continuation tokens), get with ranges, head, put (streamed into the chunker, not buffered), delete, batch delete, multipart upload, server-side copy (zero bytes copied). `versionId` on get is a commit number, so `--version-id 12` reads the file as of commit 12. `--metadata intent="..."` on a put lands in the commit log. `If-None-Match: *` makes a put create-only and fails with 412 if the key exists. Unsigned requests are refused.

## Open it in Finder (mount)

`blackhole-mount` serves a drive as a loopback NFSv3 share and mounts it. No kernel extension, no install, no sudo on macOS.

```
blackhole-mount notes ~/notes --key <key>              # read write; every save is a commit under the key
blackhole-mount notes ~/notes-0200 --key <key> --at '2026-10-26T02:00:00Z'   # read only view of the drive as it was
```

Reads fetch only the chunks touched, so a large drive opens instantly. Writes go to a journal file under the root, are chunked at disk speed 800 ms after the last write (or on rename) and committed at once; with shards on a bucket the upload runs in the background with a persistent pending list, so a drag of a large file finishes in Finder immediately, the version exists within seconds, and the bytes follow. Ctrl-C flushes, waits for pending uploads, and unmounts; a second Ctrl-C leaves them pending for the next start, which resumes them. Linux uses the same server with `mount -t nfs`; pass `--serve-only` to mount it yourself.

With shards on S3 or R2, every binary keeps a local shard cache under the root (`BLACKHOLE_CACHE_GB`, default 10, least recently used eviction). The mount prefetches the next shard of a file being read and asks the kernel for a small readahead (`--readahead`, default 4 MiB), so a video plays from a bucket without downloading it. Pin what must stay local:

```
blackhole pin corpus:/clips        # fetch every shard of the files under /clips and keep them out of eviction
blackhole pin corpus:/clips --off
blackhole evict corpus:/archive     # drop the local copies; the files stay in the bucket and stream again on demand
```

What is on your disk for a bucket backed mount is the local cache, bounded by `BLACKHOLE_CACHE_GB`, plus pinned files; `blackhole stats` shows it. The mount reports each file's cached bytes as its NFS "used" size, but the macOS NFS client derives Finder's "on disk" figure from the logical size, so Finder shows the full size there regardless. The number to trust is `stats`.

Measured with a 5 Mbit/s clip on R2 from a laptop: first frame after 2 shard fetches, 9 fetches for 20 s of playback, a seek to 240 s cost 8 more, replay from cache cost none. Finder's `._*` and `.DS_Store` files live in the mount's memory only, so `cp` and Finder work and they never become commits.

## Follow other agents

```
blackhole watch notes --after 0          # streams commits and changed paths as they land
```

The MCP `changes` tool does the same with a cursor: pass the last seq you saw. In the MCP `write` tool, `if_version` is an optimistic lock: pass the version you read, or 0 to require that the file does not exist yet, and the write fails instead of clobbering a concurrent change.

## Audit, hold and erasure

Every commit carries a hash of its own fields, of the rows it wrote, and of the previous commit, so the log is a chain. A fork's first commit chains to the parent commit it forked from.

```
blackhole verify notes            # recomputes every commit; exits 2 and names the commit if anything was altered
blackhole attest notes            # head sequence and head hash as JSON, for signing or publishing elsewhere
blackhole drive hold notes --on   # legal hold: purge is refused until released
blackhole purge notes:/x --reason "erasure request 1234"
```

Purge empties every version of a path, removes manifests nothing else references, deletes shards nothing else references, and records a tombstone commit with the reason. History still shows who wrote the file and when, marked as purged, and `verify` reports those commits as sealed with content purged rather than as tampered.

## Put the shards on S3 or R2

The authority stays local; the shard store is chosen by environment. Without these variables shards live under the root directory.

```
export BLACKHOLE_SHARDS=s3://my-bucket
export BLACKHOLE_S3_ENDPOINT=https://<account>.r2.cloudflarestorage.com   # omit for AWS
export BLACKHOLE_S3_KEY=...  BLACKHOLE_S3_SECRET=...  BLACKHOLE_S3_REGION=auto
```

Every binary reads them, so the CLI, the MCP server, the S3 endpoint and the mount share one store. `BLACKHOLE_SHARDS=file:///path` treats a directory as the bucket, with the same cache and background uploads, for tests and external disks. A version whose chunks are still uploading is served from the writer's cache and refused elsewhere with "still uploading" until they land. Measured from a laptop to R2: one 2 MiB shard per edit of a 64 MiB file at 1.2 to 2 s per put, a 1,000 file commit in 24 s, cold 100 byte reads in about 300 ms.

## How it is stored

- Files are cut with FastCDC (256 KiB / 2 MiB / 16 MiB, normalization level 2) into shards named by SHA-256 and written once to the shard store (`object_store`: local disk, S3 or R2).
- A file version is a packed manifest of shard references with an offset index, so a read finds its shard by binary search and a decoded manifest stays in memory per process. A one-byte edit to a 64 MiB file uploads one shard. A synthetic 5 TB manifest (2.6 million shards, 89 MiB) decodes in 11 ms and locates an offset in 0.1 µs.
- Names, versions and commits live in the authority (SQLite now, Postgres hosted). Rows carry `from_seq`/`to_seq`, so any path can be read at any commit or time, and history follows a file across renames.
- A fork is one row pointing at the parent drive at a sequence number. Reads fall through; writes land in the fork.
- Undo by actor restores each touched file to its state before the actor's first write, skips files another actor changed since, and records that as a new commit by whoever ran the undo. Nothing in the history is rewritten.

## Check it

```
./scripts/check.sh
```

Builds, runs the core tests (`crates/core/tests/semantics.rs`: versions, keys, undo and its conflict rules, rename over, forks and merge, conditional writes, the hash chain and tampering, hold, purge, gc, restore, the change feed), then the scenario scripts in `scripts/`: the CLI end to end, the mount with a rename soak, the S3 endpoint with the aws CLI and rclone, the single process server, and the MCP server over stdio. The S3 script needs the aws CLI and rclone; the mount script needs macOS or a Linux host where you may mount NFS.

## Known limits

- Key secrets are stored in plaintext in the authority database. Anyone who can read the database can act as any key.
- No TLS built in. Put the server behind a proxy or keep it on a private network.
- The S3 endpoint ignores `If-Match`.
- One process owns the authority. There is no clustering yet.

## Layout

```
crates/core    format (chunking, hashing, manifests), shard store and cache, authority (schema, resolution, commits, undo, chain, gc, keys)
crates/cli     blackhole
crates/mcp     blackhole-mcp (stdio) and the MCP handler library
crates/s3      blackhole-s3 and the S3 service library
crates/server  blackhole-server: S3, MCP over HTTP, change feed, key API in one process
crates/mount   blackhole-mount: loopback NFSv3
scripts/       scenario scripts and check.sh
demo/          agents.sh and fleet.sh
```

## License

Apache-2.0. See [LICENSE](LICENSE).
