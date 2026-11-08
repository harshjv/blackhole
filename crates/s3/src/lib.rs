use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use blackhole_core::{At, Authority, BhError, Ctx, Kind, Manifest, Op, Version, normalize, now_millis};
use s3s::auth::{S3Auth, SecretKey};
use s3s::dto::*;
use s3s::service::S3ServiceBuilder;
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

struct Upload {
    bucket: String,
    path: String,
    intent: String,
    parts: BTreeMap<i32, std::sync::Arc<Manifest>>,
}

#[derive(Clone)]
struct Bh {
    auth: Authority,
    uploads: Arc<Mutex<BTreeMap<String, Upload>>>,
}

fn random_id() -> String {
    use std::io::Read;
    let mut buf = [0u8; 16];
    std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut buf)).ok();
    buf.iter().map(|b| format!("{b:02x}")).collect()
}


struct KeyAuth {
    auth: Authority,
}

#[async_trait::async_trait]
impl S3Auth for KeyAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.auth.ctx_by_id(access_key).await {
            Ok(ctx) => Ok(SecretKey::from(ctx.key.secret.as_str())),
            Err(_) => Err(s3_error!(InvalidAccessKeyId)),
        }
    }
}

fn map_err(e: anyhow::Error) -> S3Error {
    match e.downcast_ref::<BhError>() {
        Some(BhError::Denied(m)) => s3_error!(AccessDenied, "{m}"),
        Some(BhError::NotFound(m)) if m.starts_with("drive ") => s3_error!(NoSuchBucket, "{m}"),
        Some(BhError::NotFound(m)) => s3_error!(NoSuchKey, "{m}"),
        Some(BhError::Conflict(m)) => s3_error!(InvalidRequest, "{m}"),
        None => S3Error::with_message(S3ErrorCode::InternalError, format!("{e:#}")),
    }
}

fn key_to_path(key: &str) -> S3Result<String> {
    normalize(key).map_err(map_err)
}

fn etag_of(v: &Version) -> Option<ETag> {
    v.manifest_hash.map(|h| format!("\"{}\"", h.to_hex()).parse().ok()).flatten()
}

fn ts(ms: i64) -> Option<Timestamp> {
    time::OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000).ok().map(Timestamp::from)
}

fn parse_version(v: &Option<String>) -> S3Result<At> {
    match v.as_deref() {
        None | Some("null") => Ok(At::Head),
        Some(s) => s.trim_start_matches('#').parse::<i64>().map(At::Seq).map_err(|_| s3_error!(InvalidArgument, "versionId must be a commit number")),
    }
}

fn intent_of(metadata: &Option<Metadata>) -> String {
    metadata.as_ref().and_then(|m| m.get("intent").cloned()).unwrap_or_default()
}

impl Bh {
    async fn ctx<T>(&self, req: &S3Request<T>) -> S3Result<Ctx> {
        let creds = req.credentials.as_ref().ok_or_else(|| s3_error!(AccessDenied, "requests must be signed with a Blackhole key"))?;
        self.auth.ctx_by_id(&creds.access_key).await.map_err(map_err)
    }

    async fn walk(&self, ctx: &Ctx, drive: &str, dir: &str, out: &mut Vec<(String, Version)>) -> Result<()> {
        for e in self.auth.list(ctx, drive, dir, At::Head).await? {
            let path = if dir == "/" { format!("/{}", e.name) } else { format!("{dir}/{}", e.name) };
            if e.version.kind == Kind::Dir {
                Box::pin(self.walk(ctx, drive, &path, out)).await?;
            } else {
                out.push((path, e.version));
            }
        }
        Ok(())
    }
}

impl Bh {
    async fn deletable(&self, ctx: &Ctx, bucket: &str, key: &str, path: &str) -> S3Result<bool> {
        let Some((_, v)) = self.auth.stat(ctx, bucket, path, At::Head).await.map_err(map_err)? else { return Ok(false) };
        if v.kind == Kind::File {
            return Ok(true);
        }
        if !key.ends_with('/') {
            return Ok(false);
        }
        let entries = self.auth.list(ctx, bucket, path, At::Head).await.map_err(map_err)?;
        Ok(entries.is_empty())
    }

    async fn listing(&self, ctx: &Ctx, bucket: &str, prefix: Option<&str>, delimiter: Option<&str>) -> S3Result<(Vec<Object>, Vec<CommonPrefix>)> {
        let prefix = prefix.unwrap_or_default().to_string();
        let (dir, rest) = match prefix.rfind('/') {
            Some(i) => (format!("/{}", &prefix[..i]), prefix[i + 1..].to_string()),
            None => ("/".to_string(), prefix.clone()),
        };
        let is_missing = |e: &anyhow::Error| e.downcast_ref::<BhError>().map(|b| matches!(b, BhError::NotFound(_))).unwrap_or(false);
        let mut contents = Vec::new();
        let mut common = Vec::new();
        if delimiter == Some("/") {
            let entries = match self.auth.list(ctx, bucket, &dir, At::Head).await {
                Ok(e) => e,
                Err(e) if is_missing(&e) => Vec::new(),
                Err(e) => return Err(map_err(e)),
            };
            for e in entries {
                if !e.name.starts_with(&rest) {
                    continue;
                }
                let path = if dir == "/" { format!("/{}", e.name) } else { format!("{dir}/{}", e.name) };
                if e.version.kind == Kind::Dir {
                    common.push(CommonPrefix { prefix: Some(format!("{}/", path.trim_start_matches('/'))) });
                } else {
                    contents.push(object_of(&path, &e.version));
                }
            }
        } else {
            let mut all = Vec::new();
            match self.walk(ctx, bucket, &dir, &mut all).await {
                Ok(()) => {}
                Err(e) if is_missing(&e) => {}
                Err(e) => return Err(map_err(e)),
            }
            for (path, v) in all {
                if path.trim_start_matches('/').starts_with(&prefix) {
                    contents.push(object_of(&path, &v));
                }
            }
        }
        Ok((contents, common))
    }
}

struct Page {
    contents: Vec<Object>,
    common: Vec<CommonPrefix>,
    truncated: bool,
    next: Option<String>,
    max_keys: i32,
}

fn paginate(contents: Vec<Object>, common: Vec<CommonPrefix>, after: Option<&str>, max_keys: Option<i32>) -> Page {
    let max_keys = max_keys.unwrap_or(1000).clamp(0, 1000);
    let mut items: Vec<(String, Option<Object>, Option<CommonPrefix>)> = Vec::with_capacity(contents.len() + common.len());
    for o in contents {
        let k = o.key.clone().unwrap_or_default();
        items.push((k, Some(o), None));
    }
    for c in common {
        let k = c.prefix.clone().unwrap_or_default();
        items.push((k, None, Some(c)));
    }
    items.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out_c = Vec::new();
    let mut out_p = Vec::new();
    let mut truncated = false;
    let mut next = None;
    let mut taken = 0;
    for (k, o, c) in items {
        if let Some(a) = after
            && k.as_str() <= a
        {
            continue;
        }
        if taken >= max_keys {
            truncated = true;
            break;
        }
        taken += 1;
        next = Some(k);
        if let Some(o) = o {
            out_c.push(o);
        } else if let Some(c) = c {
            out_p.push(c);
        }
    }
    Page { contents: out_c, common: out_p, truncated, next: if truncated { next } else { None }, max_keys }
}

fn object_of(path: &str, v: &Version) -> Object {
    Object { key: Some(path.trim_start_matches('/').to_string()), size: Some(v.size as i64), e_tag: etag_of(v), last_modified: ts(v.mtime), ..Default::default() }
}

#[async_trait::async_trait]
impl S3 for Bh {
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        let ctx = self.ctx(&req).await?;
        let mut buckets = Vec::new();
        for d in self.auth.drives().await.map_err(map_err)? {
            if self.auth.stat(&ctx, &d.alias, "/", At::Head).await.is_ok() {
                buckets.push(Bucket { name: Some(d.alias), creation_date: ts(d.created_at), ..Default::default() });
            }
        }
        Ok(S3Response::new(ListBucketsOutput { buckets: Some(buckets), ..Default::default() }))
    }

    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let ctx = self.ctx(&req).await?;
        self.auth.stat(&ctx, &req.input.bucket, "/", At::Head).await.map_err(map_err)?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        let ctx = self.ctx(&req).await?;
        let input = req.input;
        let (contents, common) = self.listing(&ctx, &input.bucket, input.prefix.as_deref(), input.delimiter.as_deref()).await?;
        let page = paginate(contents, common, input.marker.as_deref(), input.max_keys);
        Ok(S3Response::new(ListObjectsOutput {
            name: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            marker: input.marker,
            max_keys: Some(page.max_keys),
            contents: Some(page.contents),
            common_prefixes: Some(page.common),
            is_truncated: Some(page.truncated),
            next_marker: page.next,
            ..Default::default()
        }))
    }

    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        let ctx = self.ctx(&req).await?;
        let input = req.input;
        let (contents, common) = self.listing(&ctx, &input.bucket, input.prefix.as_deref(), input.delimiter.as_deref()).await?;
        let after = input.continuation_token.as_deref().or(input.start_after.as_deref());
        let page = paginate(contents, common, after, input.max_keys);
        let key_count = (page.contents.len() + page.common.len()) as i32;
        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            max_keys: Some(page.max_keys),
            continuation_token: input.continuation_token,
            start_after: input.start_after,
            contents: Some(page.contents),
            common_prefixes: Some(page.common),
            key_count: Some(key_count),
            is_truncated: Some(page.truncated),
            next_continuation_token: page.next,
            ..Default::default()
        }))
    }

    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let ctx = self.ctx(&req).await?;
        let path = key_to_path(&req.input.key)?;
        let at = parse_version(&req.input.version_id)?;
        let (_, v) = self.auth.stat(&ctx, &req.input.bucket, &path, at).await.map_err(map_err)?.ok_or_else(|| s3_error!(NoSuchKey))?;
        if (v.kind == Kind::Dir) != req.input.key.ends_with('/') {
            return Err(s3_error!(NoSuchKey));
        }
        Ok(S3Response::new(HeadObjectOutput { content_length: Some(v.size as i64), e_tag: etag_of(&v), last_modified: ts(v.mtime), ..Default::default() }))
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let ctx = self.ctx(&req).await?;
        let path = key_to_path(&req.input.key)?;
        let at = parse_version(&req.input.version_id)?;
        let (_, stat) = self.auth.stat(&ctx, &req.input.bucket, &path, at).await.map_err(map_err)?.ok_or_else(|| s3_error!(NoSuchKey))?;
        if stat.kind == Kind::Dir {
            return Err(s3_error!(NoSuchKey));
        }
        let (v, manifest) = self.auth.open_file(&ctx, &req.input.bucket, &path, at).await.map_err(map_err)?;
        let (offset, len) = match req.input.range {
            None => (0, v.size),
            Some(Range::Int { first, last }) => (first, last.map(|l| l + 1 - first).unwrap_or(v.size.saturating_sub(first))),
            Some(Range::Suffix { length }) => (v.size.saturating_sub(length), length),
        };
        if req.input.range.is_some() && offset >= v.size {
            return Err(s3_error!(InvalidRange, "requested range starts at {offset}, object is {} bytes", v.size));
        }
        let (bytes, _) = self.auth.read_range(&manifest, offset, len).await.map_err(map_err)?;
        let n = bytes.len() as i64;
        let content_range = req.input.range.as_ref().map(|_| format!("bytes {}-{}/{}", offset, offset + n.max(1) as u64 - 1, v.size));
        Ok(S3Response::new(GetObjectOutput {
            body: Some(StreamingBlob::from(bytes::Bytes::from(bytes))),
            content_length: Some(n),
            content_range,
            e_tag: etag_of(&v),
            last_modified: ts(v.mtime),
            version_id: Some(v.from_seq.to_string()),
            ..Default::default()
        }))
    }

    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let ctx = self.ctx(&req).await?;
        let input = req.input;
        let path = key_to_path(&input.key)?;
        let intent = intent_of(&input.metadata);
        if input.key.ends_with('/') {
            self.auth.commit(&ctx, &input.bucket, &intent, vec![Op::Mkdir { path }]).await.map_err(map_err)?;
            return Ok(S3Response::new(PutObjectOutput::default()));
        }
        let (hash, size) = match input.body {
            Some(body) => self.auth.put_stream(body).await.map_err(map_err)?,
            None => self.auth.put_bytes(Vec::new()).await.map_err(map_err)?,
        };
        let op = if input.if_none_match.as_ref().is_some_and(|c| c.is_any()) {
            Op::PutIf { path, manifest_hash: hash, size, mtime: now_millis(), expect_seq: 0 }
        } else {
            Op::Put { path, manifest_hash: hash, size, mtime: now_millis() }
        };
        self.auth.commit(&ctx, &input.bucket, &intent, vec![op]).await.map_err(|e| match e.downcast_ref::<BhError>() {
            Some(BhError::Conflict(m)) => s3_error!(PreconditionFailed, "{m}"),
            _ => map_err(e),
        })?;
        Ok(S3Response::new(PutObjectOutput { e_tag: format!("\"{}\"", hash.to_hex()).parse().ok(), size: Some(size as i64), ..Default::default() }))
    }

    async fn create_multipart_upload(&self, req: S3Request<CreateMultipartUploadInput>) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let ctx = self.ctx(&req).await?;
        let input = req.input;
        let path = key_to_path(&input.key)?;
        self.auth.stat(&ctx, &input.bucket, "/", At::Head).await.map_err(map_err)?;
        let id = random_id();
        self.uploads.lock().unwrap().insert(id.clone(), Upload { bucket: input.bucket.clone(), path, intent: intent_of(&input.metadata), parts: BTreeMap::new() });
        Ok(S3Response::new(CreateMultipartUploadOutput { bucket: Some(input.bucket), key: Some(input.key), upload_id: Some(id), ..Default::default() }))
    }

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        self.ctx(&req).await?;
        let input = req.input;
        if !self.uploads.lock().unwrap().contains_key(&input.upload_id) {
            return Err(s3_error!(NoSuchUpload));
        }
        let (hash, _) = match input.body {
            Some(body) => self.auth.put_stream(body).await.map_err(map_err)?,
            None => self.auth.put_bytes(Vec::new()).await.map_err(map_err)?,
        };
        let manifest = self.auth.manifest(hash).await.map_err(map_err)?;
        let mut uploads = self.uploads.lock().unwrap();
        let up = uploads.get_mut(&input.upload_id).ok_or_else(|| s3_error!(NoSuchUpload))?;
        up.parts.insert(input.part_number, manifest);
        Ok(S3Response::new(UploadPartOutput { e_tag: format!("\"{}\"", hash.to_hex()).parse().ok(), ..Default::default() }))
    }

    async fn complete_multipart_upload(&self, req: S3Request<CompleteMultipartUploadInput>) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let ctx = self.ctx(&req).await?;
        let input = req.input;
        let up = self.uploads.lock().unwrap().remove(&input.upload_id).ok_or_else(|| s3_error!(NoSuchUpload))?;
        let order: Vec<i32> = match input.multipart_upload.and_then(|m| m.parts) {
            Some(parts) => parts.iter().filter_map(|p| p.part_number).collect(),
            None => up.parts.keys().copied().collect(),
        };
        let mut refs = Vec::new();
        for n in order {
            let part = up.parts.get(&n).ok_or_else(|| s3_error!(InvalidPart, "part {n} was not uploaded"))?;
            refs.extend_from_slice(&part.shards);
        }
        let manifest = Manifest::from_shards(refs);
        let size = manifest.size();
        let hash = self.auth.store_manifest(&manifest).await.map_err(map_err)?;
        let op = Op::Put { path: up.path, manifest_hash: hash, size, mtime: now_millis() };
        self.auth.commit(&ctx, &up.bucket, &up.intent, vec![op]).await.map_err(map_err)?;
        Ok(S3Response::new(CompleteMultipartUploadOutput { bucket: Some(input.bucket), key: Some(input.key), e_tag: format!("\"{}\"", hash.to_hex()).parse().ok(), ..Default::default() }))
    }

    async fn abort_multipart_upload(&self, req: S3Request<AbortMultipartUploadInput>) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.ctx(&req).await?;
        self.uploads.lock().unwrap().remove(&req.input.upload_id).ok_or_else(|| s3_error!(NoSuchUpload))?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let ctx = self.ctx(&req).await?;
        let path = key_to_path(&req.input.key)?;
        if self.deletable(&ctx, &req.input.bucket, &req.input.key, &path).await? {
            self.auth.commit(&ctx, &req.input.bucket, "", vec![Op::Delete { path }]).await.map_err(map_err)?;
        }
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let ctx = self.ctx(&req).await?;
        let mut ops = Vec::new();
        let mut deleted = Vec::new();
        for o in req.input.delete.objects {
            let path = key_to_path(&o.key)?;
            if self.deletable(&ctx, &req.input.bucket, &o.key, &path).await? {
                ops.push(Op::Delete { path });
            }
            deleted.push(DeletedObject { key: Some(o.key), ..Default::default() });
        }
        if !ops.is_empty() {
            self.auth.commit(&ctx, &req.input.bucket, "", ops).await.map_err(map_err)?;
        }
        Ok(S3Response::new(DeleteObjectsOutput { deleted: Some(deleted), ..Default::default() }))
    }

    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let ctx = self.ctx(&req).await?;
        let input = req.input;
        let (src_bucket, src_key, src_version) = match &input.copy_source {
            CopySource::Bucket { bucket, key, version_id } => (bucket.to_string(), key.to_string(), version_id.as_ref().map(|v| v.to_string())),
            _ => return Err(s3_error!(NotImplemented, "access point copy sources")),
        };
        let src_path = key_to_path(&src_key)?;
        let at = parse_version(&src_version)?;
        let (_, v) = self.auth.stat(&ctx, &src_bucket, &src_path, at).await.map_err(map_err)?.ok_or_else(|| s3_error!(NoSuchKey))?;
        if v.kind != Kind::File {
            return Err(s3_error!(InvalidRequest, "source is a directory"));
        }
        let dest = key_to_path(&input.key)?;
        let op = Op::Put { path: dest, manifest_hash: v.manifest_hash.unwrap(), size: v.size, mtime: now_millis() };
        self.auth.commit(&ctx, &input.bucket, &intent_of(&input.metadata), vec![op]).await.map_err(map_err)?;
        Ok(S3Response::new(CopyObjectOutput { copy_object_result: Some(CopyObjectResult { e_tag: etag_of(&v), last_modified: ts(now_millis()), ..Default::default() }), ..Default::default() }))
    }
}

pub fn service(auth: Authority) -> s3s::service::S3Service {
    let mut builder = S3ServiceBuilder::new(Bh { auth: auth.clone(), uploads: Arc::new(Mutex::new(BTreeMap::new())) });
    builder.set_auth(KeyAuth { auth });
    builder.build()
}
