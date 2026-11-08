use blackhole_core::{At, Authority, BhError, Ctx, Kind, Op, now_millis};
use sqlx::Row;

struct World {
    auth: Authority,
    owner: Ctx,
    _dir: tempfile::TempDir,
}

async fn world() -> World {
    let dir = tempfile::tempdir().unwrap();
    let auth = Authority::open(dir.path()).await.unwrap();
    let (_, key) = auth.create_drive("d", "owner").await.unwrap();
    let owner = auth.ctx(&key.secret).await.unwrap();
    World { auth, owner, _dir: dir }
}

async fn actor(w: &World, name: &str, prefix: Option<&str>) -> Ctx {
    let k = w.auth.create_key(&w.owner, "d", name, prefix, false, false, None).await.unwrap();
    w.auth.ctx(&k.secret).await.unwrap()
}

async fn write(w: &World, ctx: &Ctx, path: &str, content: &str, intent: &str) -> i64 {
    let (hash, size) = w.auth.put_bytes(content.as_bytes().to_vec()).await.unwrap();
    let op = Op::Put { path: path.to_string(), manifest_hash: hash, size, mtime: now_millis() };
    w.auth.commit(ctx, "d", intent, vec![op]).await.unwrap().seq
}

async fn read(w: &World, ctx: &Ctx, path: &str, at: At) -> Option<String> {
    match w.auth.open_file(ctx, "d", path, at).await {
        Ok((v, m)) => {
            let (bytes, _) = w.auth.read_range(&m, 0, v.size).await.unwrap();
            Some(String::from_utf8(bytes).unwrap())
        }
        Err(_) => None,
    }
}

fn is_denied(e: &anyhow::Error) -> bool {
    matches!(e.downcast_ref::<BhError>(), Some(BhError::Denied(_)))
}

#[tokio::test]
async fn every_write_is_a_version_and_reads_at_any_commit() {
    let w = world().await;
    let s1 = write(&w, &w.owner, "/a.txt", "one", "first").await;
    let s2 = write(&w, &w.owner, "/a.txt", "two", "second").await;
    assert_eq!(read(&w, &w.owner, "/a.txt", At::Head).await.as_deref(), Some("two"));
    assert_eq!(read(&w, &w.owner, "/a.txt", At::Seq(s1)).await.as_deref(), Some("one"));
    assert_eq!(read(&w, &w.owner, "/a.txt", At::Seq(s2)).await.as_deref(), Some("two"));
    let history = w.auth.history(&w.owner, "d", "/a.txt").await.unwrap();
    assert_eq!(history.iter().filter(|e| e.what.starts_with("write")).count(), 2);
    assert!(history.iter().all(|e| e.actor == "owner"));
}

#[tokio::test]
async fn keys_are_scoped_to_drive_prefix_and_mode() {
    let w = world().await;
    write(&w, &w.owner, "/docs/a.txt", "x", "seed").await;
    write(&w, &w.owner, "/other/b.txt", "y", "seed").await;
    let scoped = actor(&w, "agent", Some("/docs")).await;
    write(&w, &scoped, "/docs/c.txt", "ok", "inside prefix").await;
    let (hash, size) = w.auth.put_bytes(b"no".to_vec()).await.unwrap();
    let outside = w.auth.commit(&scoped, "d", "", vec![Op::Put { path: "/other/c.txt".into(), manifest_hash: hash, size, mtime: 0 }]).await;
    assert!(is_denied(&outside.unwrap_err()));
    let ro_key = w.auth.create_key(&w.owner, "d", "auditor", None, true, false, None).await.unwrap();
    let ro = w.auth.ctx(&ro_key.secret).await.unwrap();
    assert_eq!(read(&w, &ro, "/docs/a.txt", At::Head).await.as_deref(), Some("x"));
    let denied = w.auth.commit(&ro, "d", "", vec![Op::Put { path: "/docs/z.txt".into(), manifest_hash: hash, size, mtime: 0 }]).await;
    assert!(is_denied(&denied.unwrap_err()));
    let expired_key = w.auth.create_key(&w.owner, "d", "short", None, false, false, Some(-1)).await.unwrap();
    let expired = w.auth.ctx(&expired_key.secret).await.unwrap();
    assert!(is_denied(&w.auth.stat(&expired, "d", "/docs/a.txt", At::Head).await.unwrap_err()));
    assert!(w.auth.ctx(&expired_key.id).await.is_err(), "the id must not work as a secret");
}

#[tokio::test]
async fn undo_reverts_one_actor_and_reports_conflicts() {
    let w = world().await;
    write(&w, &w.owner, "/r/a.txt", "a0", "seed").await;
    write(&w, &w.owner, "/r/b.txt", "b0", "seed").await;
    let agent = actor(&w, "agent-2", None).await;
    let since = now_millis() - 1;
    write(&w, &agent, "/r/a.txt", "a1", "agent rewrites a").await;
    write(&w, &agent, "/r/c.txt", "c1", "agent adds c").await;
    w.auth.commit(&agent, "d", "agent deletes b", vec![Op::Delete { path: "/r/b.txt".into() }]).await.unwrap();
    write(&w, &w.owner, "/r/b.txt", "b2", "owner recreates b").await;
    write(&w, &w.owner, "/r/d.txt", "d0", "owner keeps working").await;
    let report = w.auth.undo(&w.owner, "d", "agent-2", since).await.unwrap();
    assert!(report.commit.is_some());
    assert_eq!(read(&w, &w.owner, "/r/a.txt", At::Head).await.as_deref(), Some("a0"));
    assert_eq!(read(&w, &w.owner, "/r/c.txt", At::Head).await, None);
    assert_eq!(read(&w, &w.owner, "/r/b.txt", At::Head).await.as_deref(), Some("b2"), "the owner's later recreation stays");
    assert_eq!(read(&w, &w.owner, "/r/d.txt", At::Head).await.as_deref(), Some("d0"));
    assert!(report.conflicts.iter().any(|p| p == "/r/b.txt"));
}

#[tokio::test]
async fn undo_does_not_wipe_other_actors_files_inside_a_directory_the_actor_created() {
    let w = world().await;
    let agent = actor(&w, "agent-8", None).await;
    let since = now_millis() - 1;
    write(&w, &agent, "/shared/x.txt", "x", "agent creates dir and file").await;
    write(&w, &w.owner, "/shared/owner.txt", "o", "owner adds a file in it").await;
    let report = w.auth.undo(&w.owner, "d", "agent-8", since).await.unwrap();
    assert_eq!(read(&w, &w.owner, "/shared/x.txt", At::Head).await, None);
    assert_eq!(read(&w, &w.owner, "/shared/owner.txt", At::Head).await.as_deref(), Some("o"));
    assert!(report.conflicts.iter().any(|p| p == "/shared/owner.txt"));
}

#[tokio::test]
async fn rename_over_keeps_the_targets_identity_and_history() {
    let w = world().await;
    write(&w, &w.owner, "/doc.md", "draft 1", "draft 1").await;
    write(&w, &w.owner, "/doc.tmp", "draft 2", "editor temp").await;
    w.auth.commit(&w.owner, "d", "atomic save", vec![Op::Rename { from: "/doc.tmp".into(), to: "/doc.md".into() }]).await.unwrap();
    assert_eq!(read(&w, &w.owner, "/doc.md", At::Head).await.as_deref(), Some("draft 2"));
    assert_eq!(read(&w, &w.owner, "/doc.tmp", At::Head).await, None);
    let history = w.auth.history(&w.owner, "d", "/doc.md").await.unwrap();
    assert_eq!(history.iter().filter(|e| e.what.starts_with("write")).count(), 2);
}

#[tokio::test]
async fn fork_is_one_row_reads_fall_through_and_keys_do_not_cross() {
    let w = world().await;
    write(&w, &w.owner, "/a.txt", "parent", "seed").await;
    let (fork, fkey) = w.auth.fork(&w.owner, "d", "d-fork").await.unwrap();
    let fctx = w.auth.ctx(&fkey.secret).await.unwrap();
    assert_eq!(fork.fork_seq, Some(1));
    let (v, m) = w.auth.open_file(&fctx, "d-fork", "/a.txt", At::Head).await.unwrap();
    let (bytes, _) = w.auth.read_range(&m, 0, v.size).await.unwrap();
    assert_eq!(bytes, b"parent");
    assert!(is_denied(&w.auth.stat(&fctx, "d", "/a.txt", At::Head).await.unwrap_err()), "fork key cannot read the parent");
    let agent = actor(&w, "agent", None).await;
    assert!(is_denied(&w.auth.stat(&agent, "d-fork", "/a.txt", At::Head).await.unwrap_err()), "non-admin parent key cannot read the fork");
    assert!(w.auth.stat(&w.owner, "d-fork", "/a.txt", At::Head).await.is_ok(), "owner admin key can review the fork");
    let (hash, size) = w.auth.put_bytes(b"fork".to_vec()).await.unwrap();
    w.auth.commit(&fctx, "d-fork", "fork edit", vec![Op::Put { path: "/a.txt".into(), manifest_hash: hash, size, mtime: 0 }]).await.unwrap();
    assert_eq!(read(&w, &w.owner, "/a.txt", At::Head).await.as_deref(), Some("parent"));
    let changes = w.auth.fork_changes(&w.owner, "d-fork").await.unwrap().2;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind, "modified");
    let merged = w.auth.merge(&w.owner, "d-fork").await.unwrap();
    assert!(merged.commit.is_some());
    assert_eq!(read(&w, &w.owner, "/a.txt", At::Head).await.as_deref(), Some("fork"));
}

#[tokio::test]
async fn conditional_writes_let_exactly_one_writer_win() {
    let w = world().await;
    let (hash, size) = w.auth.put_bytes(b"first".to_vec()).await.unwrap();
    let create_only = |expect| Op::PutIf { path: "/lock".into(), manifest_hash: hash, size, mtime: 0, expect_seq: expect };
    w.auth.commit(&w.owner, "d", "", vec![create_only(0)]).await.unwrap();
    let second = w.auth.commit(&w.owner, "d", "", vec![create_only(0)]).await;
    assert!(matches!(second.unwrap_err().downcast_ref::<BhError>(), Some(BhError::Conflict(_))));
    let stale = w.auth.commit(&w.owner, "d", "", vec![create_only(99)]).await;
    assert!(matches!(stale.unwrap_err().downcast_ref::<BhError>(), Some(BhError::Conflict(_))));
}

#[tokio::test]
async fn chain_verifies_and_detects_tampering() {
    let w = world().await;
    write(&w, &w.owner, "/a.txt", "one", "first").await;
    write(&w, &w.owner, "/a.txt", "two", "second").await;
    let lines = w.auth.verify(&w.owner, "d").await.unwrap();
    assert!(lines.iter().all(|l| l.status == "ok"), "{lines:?}");
    sqlx::query("UPDATE \"commit\" SET intent = 'forged' WHERE seq = 1").execute(&w.auth.pool).await.unwrap();
    let lines = w.auth.verify(&w.owner, "d").await.unwrap();
    assert!(lines[0].status.starts_with("bad header"), "{lines:?}");
    assert_eq!(lines[1].status, "ok");
    sqlx::query("UPDATE \"commit\" SET intent = 'first' WHERE seq = 1").execute(&w.auth.pool).await.unwrap();
    sqlx::query("UPDATE node_version SET size = size + 1 WHERE from_seq = 2").execute(&w.auth.pool).await.unwrap();
    let lines = w.auth.verify(&w.owner, "d").await.unwrap();
    assert!(lines[1].status.starts_with("bad rows"), "{lines:?}");
}

#[tokio::test]
async fn forks_chain_to_the_parent_commit() {
    let w = world().await;
    write(&w, &w.owner, "/a.txt", "one", "first").await;
    let (_, fkey) = w.auth.fork(&w.owner, "d", "d-fork").await.unwrap();
    let fctx = w.auth.ctx(&fkey.secret).await.unwrap();
    let (hash, size) = w.auth.put_bytes(b"f".to_vec()).await.unwrap();
    w.auth.commit(&fctx, "d-fork", "fork write", vec![Op::Put { path: "/f.txt".into(), manifest_hash: hash, size, mtime: 0 }]).await.unwrap();
    let parent_head: Vec<u8> = sqlx::query("SELECT hash FROM \"commit\" WHERE drive_id = 1 AND seq = 1").fetch_one(&w.auth.pool).await.unwrap().get("hash");
    let fork_prev: Vec<u8> = sqlx::query("SELECT prev_hash FROM \"commit\" WHERE drive_id = 2 AND seq = 1").fetch_one(&w.auth.pool).await.unwrap().get("prev_hash");
    assert_eq!(parent_head, fork_prev);
    assert!(w.auth.verify(&fctx, "d-fork").await.unwrap().iter().all(|l| l.status == "ok"));
}

#[tokio::test]
async fn hold_blocks_purge_and_purge_keeps_the_record() {
    let w = world().await;
    write(&w, &w.owner, "/secret.txt", "pii", "write").await;
    w.auth.set_hold(&w.owner, "d", true).await.unwrap();
    let refused = w.auth.purge(&w.owner, "d", "/secret.txt", "gdpr").await;
    assert!(matches!(refused.unwrap_err().downcast_ref::<BhError>(), Some(BhError::Conflict(_))));
    w.auth.set_hold(&w.owner, "d", false).await.unwrap();
    let before = w.auth.stats().await.unwrap().shards;
    let r = w.auth.purge(&w.owner, "d", "/secret.txt", "gdpr").await.unwrap();
    assert_eq!(r.versions_purged, 1);
    assert_eq!(w.auth.stats().await.unwrap().shards, before - 1);
    assert_eq!(read(&w, &w.owner, "/secret.txt", At::Seq(1)).await, None);
    let history = w.auth.history(&w.owner, "d", "/secret.txt").await.unwrap();
    assert!(history.iter().any(|e| e.what.contains("purged")));
    let lines = w.auth.verify(&w.owner, "d").await.unwrap();
    assert!(lines.iter().all(|l| l.status.starts_with("ok")), "{lines:?}");
}

#[tokio::test]
async fn gc_keeps_recent_versions_and_fork_pinned_ones() {
    let w = world().await;
    for i in 1..=3 {
        write(&w, &w.owner, "/v.txt", &format!("rev {i}"), "rev").await;
    }
    let (_, fkey) = w.auth.fork(&w.owner, "d", "d-pin").await.unwrap();
    let fctx = w.auth.ctx(&fkey.secret).await.unwrap();
    for i in 4..=7 {
        write(&w, &w.owner, "/v.txt", &format!("rev {i}"), "rev").await;
    }
    let r = w.auth.gc(&w.owner, "d", 2, None).await.unwrap();
    assert_eq!(r.versions_expired, 3, "closed 1..6, keep 5 and 6, fork pins 3, so 1, 2 and 4 expire: {r:?}");
    assert_eq!(read(&w, &w.owner, "/v.txt", At::Head).await.as_deref(), Some("rev 7"));
    let (v, m) = w.auth.open_file(&fctx, "d-pin", "/v.txt", At::Head).await.unwrap();
    let (bytes, _) = w.auth.read_range(&m, 0, v.size).await.unwrap();
    assert_eq!(bytes, b"rev 3");
    assert_eq!(read(&w, &w.owner, "/v.txt", At::Seq(3)).await.as_deref(), Some("rev 3"));
    assert_eq!(read(&w, &w.owner, "/v.txt", At::Seq(4)).await, None);
    assert_eq!(read(&w, &w.owner, "/v.txt", At::Seq(1)).await, None);
    assert!(w.auth.verify(&w.owner, "d").await.unwrap().iter().all(|l| l.status.starts_with("ok")));
}

#[tokio::test]
async fn restore_a_directory_to_an_earlier_commit() {
    let w = world().await;
    write(&w, &w.owner, "/p/a.txt", "a", "seed").await;
    let at = write(&w, &w.owner, "/p/b.txt", "b", "seed").await;
    w.auth.commit(&w.owner, "d", "delete", vec![Op::Delete { path: "/p/a.txt".into() }]).await.unwrap();
    write(&w, &w.owner, "/p/b.txt", "b2", "edit").await;
    write(&w, &w.owner, "/p/c.txt", "c", "add").await;
    w.auth.restore(&w.owner, "d", "/p", At::Seq(at)).await.unwrap().unwrap();
    let names: Vec<String> = w.auth.list(&w.owner, "d", "/p", At::Head).await.unwrap().into_iter().map(|e| e.name).collect();
    assert_eq!(names, vec!["a.txt", "b.txt"]);
    assert_eq!(read(&w, &w.owner, "/p/b.txt", At::Head).await.as_deref(), Some("b"));
    assert!(w.auth.list(&w.owner, "d", "/p", At::Head).await.unwrap().iter().all(|e| e.version.kind == Kind::File));
}

#[tokio::test]
async fn change_feed_reports_actor_and_paths() {
    let w = world().await;
    let agent = actor(&w, "agent-1", None).await;
    write(&w, &agent, "/x/y.txt", "1", "first").await;
    w.auth.commit(&agent, "d", "drop", vec![Op::Delete { path: "/x/y.txt".into() }]).await.unwrap();
    let sets = w.auth.changes_since(&w.owner, "d", 0, 10).await.unwrap();
    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0].commit.actor, "agent-1");
    assert!(sets[0].paths.iter().any(|(p, k)| p == "/x/y.txt" && k == "write"));
    assert!(sets[1].paths.iter().any(|(p, k)| p == "/x/y.txt" && k == "unlink"));
}

#[tokio::test]
async fn a_version_whose_chunks_are_still_uploading_is_not_served_to_a_cold_reader() {
    let remote = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("BLACKHOLE_SHARDS", format!("file://{}", remote.path().display()));
        std::env::set_var("BLACKHOLE_CACHE_GB", "1");
    }
    let auth = Authority::open(dir.path()).await.unwrap();
    unsafe {
        std::env::remove_var("BLACKHOLE_SHARDS");
        std::env::remove_var("BLACKHOLE_CACHE_GB");
    }
    let (_, key) = auth.create_drive("d", "owner").await.unwrap();
    let owner = auth.ctx(&key.secret).await.unwrap();
    let (hash, size) = auth.put_bytes(b"payload".to_vec()).await.unwrap();
    auth.commit(&owner, "d", "write", vec![Op::Put { path: "/p.txt".into(), manifest_hash: hash, size, mtime: 0 }]).await.unwrap();
    auth.store.drain().await.unwrap();
    let (v, m) = auth.open_file(&owner, "d", "/p.txt", At::Head).await.unwrap();
    let shard = m.shards[0].hash;
    assert_eq!(auth.read_range(&m, 0, v.size).await.unwrap().0, b"payload");
    sqlx::query("INSERT INTO pending_upload(hash, since) VALUES (?, 0)").bind(shard.0.to_vec()).execute(&auth.pool).await.unwrap();
    auth.store.cache().unwrap().evict(std::iter::once(shard));
    let err = auth.read_range(&m, 0, v.size).await.unwrap_err();
    assert!(matches!(err.downcast_ref::<BhError>(), Some(BhError::Conflict(m)) if m.contains("uploading")), "{err:#}");
    sqlx::query("DELETE FROM pending_upload WHERE hash = ?").bind(shard.0.to_vec()).execute(&auth.pool).await.unwrap();
    assert_eq!(auth.read_range(&m, 0, v.size).await.unwrap().0, b"payload");
}

#[tokio::test]
async fn a_fork_read_as_of_before_its_fork_point_resolves_to_the_parent_for_both_keys() {
    let w = world().await;
    let t0 = now_millis();
    write(&w, &w.owner, "/a.txt", "before", "seed").await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let t_mid = now_millis();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    write(&w, &w.owner, "/a.txt", "later", "edit").await;
    let (_, fkey) = w.auth.fork(&w.owner, "d", "d-fork").await.unwrap();
    let fctx = w.auth.ctx(&fkey.secret).await.unwrap();
    for ctx in [&w.owner, &fctx] {
        let (v, m) = w.auth.open_file(ctx, "d-fork", "/a.txt", At::Time(t_mid)).await.unwrap();
        let (bytes, _) = w.auth.read_range(&m, 0, v.size).await.unwrap();
        assert_eq!(bytes, b"before");
        assert!(w.auth.list(ctx, "d-fork", "/", At::Time(t0 - 1)).await.unwrap().is_empty());
    }
}
