use std::collections::{HashMap, HashSet};

use nix_compat::{
    narinfo::{Flags, NarInfo},
    nixhash,
    store_path::StorePathRef,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct ClosureInner {
    #[serde(rename = "narHash")]
    nar_hash: String,
    #[serde(rename = "narSize")]
    nar_size: u64,
    path: String,
    references: Vec<String>,
}

impl ClosureInner {
    fn to_narinfo(&self) -> Result<NarInfo, Box<dyn std::error::Error>> {
        Ok(NarInfo {
            flags: Flags::empty(),
            store_path: StorePathRef::from_absolute_path(self.path.as_bytes())?,
            nar_hash: match nixhash::from_nix_str(&self.nar_hash) {
                Ok(nixhash::NixHash::Sha256(h)) => h,
                _ => Err("Failed to parse Nix hash string")?,
            },
            nar_size: self.nar_size,
            references: self
                .references
                .iter()
                .map(|r| StorePathRef::from_absolute_path(r.as_bytes()))
                .collect::<Result<_, _>>()?,
            signatures: Default::default(),
            ca: Default::default(),
            system: Default::default(),
            deriver: Default::default(),
            url: Default::default(),
            compression: Default::default(),
            file_hash: Default::default(),
            file_size: Default::default(),
        })
    }
}

#[derive(Deserialize)]
pub struct Closure {
    closure: Vec<ClosureInner>,
}

impl Closure {
    pub fn to_narinfos(&self) -> HashMap<StorePathRef<'_>, NarInfo<'_>> {
        self.closure
            .iter()
            .map(|c| {
                let c = c.to_narinfo().unwrap();
                (c.store_path, c)
            })
            .collect()
    }
}

pub fn make_work<'a>(
    mut narinfo_map: HashMap<StorePathRef<'a>, NarInfo<'a>>,
) -> Vec<HashSet<StorePathRef<'a>>> {
    let mut worklist: Vec<HashSet<StorePathRef>> = Vec::new();
    while !narinfo_map.is_empty() {
        let mut chunk = HashSet::new();
        narinfo_map.retain(|s, n| {
            if n.references.is_empty()
                || (n.references.len() == 1 && n.references.contains(s))
                || n.references
                    .iter()
                    .all(|r| r == s || worklist.iter().any(|x| x.contains(r)))
            {
                chunk.insert(*s);
                false
            } else {
                true
            }
        });
        worklist.push(chunk);
    }

    worklist
}

pub fn do_work(worklist: Vec<HashSet<StorePathRef<'_>>>) -> HashSet<StorePathRef<'_>> {
    let mut uploaded = HashSet::new();
    for chunk in worklist {
        for store_path in chunk {
            uploaded.insert(store_path);
        }
    }
    uploaded
}

#[cfg(test)]
mod tests {
    use crate::{do_work, make_work, Closure};
    use rstest::rstest;
    use std::collections::HashSet;
    use std::path::PathBuf;

    #[rstest]
    fn check_order(#[files("src/fixtures/*.json")] fixture_path: PathBuf) {
        let closure: Closure =
            serde_json::from_slice(&std::fs::read(fixture_path).unwrap()).unwrap();
        let nar_infos = closure.to_narinfos();

        let worklist = make_work(nar_infos);
        let last_not_in_all_previous = worklist.last().unwrap().iter().all(|x| {
            worklist
                .iter()
                .take(worklist.len() - 1)
                .all(|y| !y.contains(x))
        });

        assert!(last_not_in_all_previous);
    }

    #[rstest]
    fn all_references_uploaded(#[files("src/fixtures/*.json")] fixture_path: PathBuf) {
        let closure: Closure =
            serde_json::from_slice(&std::fs::read(fixture_path).unwrap()).unwrap();
        let nar_infos = closure.to_narinfos();

        let all_references = nar_infos
            .values()
            .flat_map(|x| x.references.clone())
            .collect::<HashSet<_>>();
        let uploaded = do_work(make_work(nar_infos));

        assert_eq!(all_references, uploaded);
    }
}
