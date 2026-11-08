use std::fmt;
use std::io::Read;
use std::sync::mpsc;

use anyhow::{Context, Result, bail};
use fastcdc::v2020::{Normalization, StreamCDC};
use rayon::prelude::*;
use sha2::{Digest, Sha256};

pub const SHARD_MIN: usize = 256 * 1024;
pub const SHARD_AVG: usize = 2 * 1024 * 1024;
pub const SHARD_MAX: usize = 16 * 1024 * 1024;
pub const CHUNK_SEED: u64 = 0;
const MANIFEST_MAGIC: &[u8; 4] = b"BHM1";

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    pub fn of(bytes: &[u8]) -> Hash {
        Hash(Sha256::digest(bytes).into())
    }

    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> Result<Hash> {
        let v = hex::decode(s).with_context(|| format!("bad hash {s}"))?;
        let arr: [u8; 32] = v.try_into().map_err(|_| anyhow::anyhow!("bad hash length {s}"))?;
        Ok(Hash(arr))
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardRef {
    pub hash: Hash,
    pub len: u32,
}

pub struct Shard {
    pub hash: Hash,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Manifest {
    pub shards: Vec<ShardRef>,
    ends: Vec<u64>,
}

impl Manifest {
    pub fn from_shards(shards: Vec<ShardRef>) -> Manifest {
        let mut ends = Vec::with_capacity(shards.len());
        let mut pos = 0u64;
        for s in &shards {
            pos += s.len as u64;
            ends.push(pos);
        }
        Manifest { shards, ends }
    }

    pub fn size(&self) -> u64 {
        self.ends.last().copied().unwrap_or(0)
    }

    pub fn offset_of(&self, i: usize) -> u64 {
        if i == 0 { 0 } else { self.ends[i - 1] }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.shards.len() * 36);
        out.extend_from_slice(MANIFEST_MAGIC);
        out.extend_from_slice(&(self.shards.len() as u32).to_le_bytes());
        for s in &self.shards {
            out.extend_from_slice(&s.hash.0);
            out.extend_from_slice(&s.len.to_le_bytes());
        }
        out
    }

    pub fn decode(body: &[u8]) -> Result<Manifest> {
        if body.len() < 8 || &body[..4] != MANIFEST_MAGIC {
            bail!("not a manifest");
        }
        let count = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
        if body.len() != 8 + count * 36 {
            bail!("manifest length mismatch");
        }
        let shards = body[8..]
            .chunks_exact(36)
            .map(|c| ShardRef {
                hash: Hash(c[..32].try_into().unwrap()),
                len: u32::from_le_bytes(c[32..36].try_into().unwrap()),
            })
            .collect();
        Ok(Manifest::from_shards(shards))
    }

    pub fn hash(&self) -> Hash {
        Hash::of(&self.encode())
    }

    pub fn locate(&self, offset: u64) -> Option<(usize, u64)> {
        if offset >= self.size() {
            return None;
        }
        let i = self.ends.partition_point(|end| *end <= offset);
        Some((i, offset - self.offset_of(i)))
    }
}

pub fn chunk<R: Read + Send + 'static>(reader: R, mut sink: impl FnMut(Shard) -> Result<()>) -> Result<Manifest> {
    let (tx, rx) = mpsc::sync_channel::<Vec<Vec<u8>>>(4);
    let chunker = std::thread::spawn(move || -> Result<()> {
        let cdc = StreamCDC::with_level_and_seed(reader, SHARD_MIN, SHARD_AVG, SHARD_MAX, Normalization::Level2, CHUNK_SEED);
        let mut batch = Vec::with_capacity(8);
        for item in cdc {
            let item = item.context("chunking")?;
            batch.push(item.data);
            if batch.len() == 8 {
                tx.send(std::mem::take(&mut batch)).ok();
            }
        }
        if !batch.is_empty() {
            tx.send(batch).ok();
        }
        Ok(())
    });
    let mut refs = Vec::new();
    for batch in rx {
        let hashed: Vec<Shard> = batch
            .into_par_iter()
            .map(|data| Shard { hash: Hash::of(&data), data })
            .collect();
        for shard in hashed {
            refs.push(ShardRef { hash: shard.hash, len: shard.data.len() as u32 });
            sink(shard)?;
        }
    }
    chunker.join().map_err(|_| anyhow::anyhow!("chunker thread panicked"))??;
    Ok(Manifest::from_shards(refs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_roundtrip() {
        let m = Manifest::from_shards(vec![ShardRef { hash: Hash::of(b"a"), len: 1 }, ShardRef { hash: Hash::of(b"b"), len: 2 }]);
        assert_eq!(Manifest::decode(&m.encode()).unwrap(), m);
        assert_eq!(m.locate(1), Some((1, 0)));
        assert_eq!(m.locate(3), None);
    }

    #[test]
    fn chunk_small_file_is_one_shard() {
        let data = vec![7u8; 1000];
        let mut shards = Vec::new();
        let m = chunk(std::io::Cursor::new(data.clone()), |s| {
            shards.push(s);
            Ok(())
        })
        .unwrap();
        assert_eq!(m.shards.len(), 1);
        assert_eq!(shards[0].data, data);
        assert_eq!(m.size(), 1000);
    }

    #[test]
    fn chunk_empty_file() {
        let m = chunk(std::io::Cursor::new(Vec::<u8>::new()), |_| Ok(())).unwrap();
        assert!(m.shards.is_empty());
    }
}

#[cfg(test)]
mod scale {
    use super::*;

    #[test]
    #[ignore]
    fn five_terabyte_manifest() {
        let n = 2_600_000usize;
        let shards: Vec<ShardRef> = (0..n).map(|i| ShardRef { hash: Hash::of(&(i as u64).to_le_bytes()), len: 2 * 1024 * 1024 }).collect();
        let t = std::time::Instant::now();
        let m = Manifest::from_shards(shards);
        let build = t.elapsed();
        let t = std::time::Instant::now();
        let body = m.encode();
        let encode = t.elapsed();
        let t = std::time::Instant::now();
        let m2 = Manifest::decode(&body).unwrap();
        let decode = t.elapsed();
        let size = m2.size();
        let t = std::time::Instant::now();
        let mut hits = 0u64;
        for k in 0..100_000u64 {
            let off = (k * 7_919_000_003) % size;
            let (i, inner) = m2.locate(off).unwrap();
            hits += i as u64 + inner;
        }
        let locate = t.elapsed();
        println!("5 TB manifest: {n} shards, body {} MiB, size {} TB, build {:?}, encode {:?}, decode {:?}, 100k locates {:?} ({:.1} us each) [{hits}]", body.len() / 1048576, size / 1_000_000_000_000, build, encode, decode, locate, locate.as_secs_f64() * 10.0);
        assert_eq!(m2.locate(0), Some((0, 0)));
        assert_eq!(m2.locate(size - 1), Some((n - 1, 2 * 1024 * 1024 - 1)));
        assert_eq!(m2.locate(size), None);
    }
}
