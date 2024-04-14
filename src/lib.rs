use nix_compat::path_info::ExportedPathInfo;
use petgraph::{graphmap::DiGraphMap, visit::Topo};
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Deserialize)]
pub struct Closure<'a> {
    #[serde(borrow)]
    pub(self) closure: Vec<ExportedPathInfo<'a>>,
}

pub struct ClosureGraph<'a>(DiGraphMap<&'a ExportedPathInfo<'a>, ()>);

impl ClosureGraph<'_> {
    /// Uploads all the [`StorePathRef`]'s in the [`ClosureGraph`] and returns them in a [`HashSet`].
    pub fn upload_all(&self) -> HashSet<&ExportedPathInfo> {
        let mut topo = Topo::new(&self.0);
        let mut uploaded = HashSet::new();
        // We use Topological Sorting to sort the graph and upload the store path as soon as it gets sorted.
        while let Some(path_info) = topo.next(&self.0) {
            uploaded.insert(path_info);
            println!("Uploaded {}", path_info.path);
        }
        uploaded
    }
}

impl<'a> From<&'a Closure<'a>> for ClosureGraph<'a> {
    /// Creates a new [`ClosureGraph`] from a [`Closure`]
    fn from(c: &'a Closure<'a>) -> ClosureGraph<'a> {
        // Create edges from nodes (store paths) that are referenced by other nodes.
        let edges = c.closure.iter().flat_map(|target_path| {
            c.closure
                .iter()
                .filter(|source_path| {
                    source_path.path != target_path.path // Avoid self references.
                    && target_path.references.contains(&source_path.path)
                })
                .map(move |source_path| (source_path, target_path))
        });

        // Construct the graph directly from the iterator of edges.
        Self(DiGraphMap::from_edges(edges))
    }
}

#[cfg(test)]
mod tests {
    use crate::{Closure, ClosureGraph};
    use rstest::rstest;
    use std::{collections::HashSet, path::PathBuf};

    #[rstest]
    fn all_references_uploaded(#[files("src/fixtures/*.json")] fixture_path: PathBuf) {
        let json_data = std::fs::read(fixture_path).unwrap();
        let closure: Closure = serde_json::from_slice(&json_data).unwrap();
        let graph = ClosureGraph::from(&closure);

        // These are all the store that we expect to get uploaded.
        let all_paths = closure.closure.iter().collect::<HashSet<_>>();

        let uploaded_paths = graph.upload_all();

        // We check that all the paths indeed end up getting uploaded.
        assert_eq!(all_paths, uploaded_paths);
    }
}
