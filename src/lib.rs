use futures::{stream, StreamExt};
use nix_compat::path_info::ExportedPathInfo;
use petgraph::{
    algo::{toposort, Cycle},
    graphmap::DiGraphMap,
};
use serde::Deserialize;
use std::collections::HashSet;
use tokio::sync::RwLock;

#[derive(Deserialize)]
pub struct Closure<'a> {
    #[serde(borrow)]
    pub(self) closure: Vec<ExportedPathInfo<'a>>,
}

pub struct ClosureGraph<'a>(DiGraphMap<&'a ExportedPathInfo<'a>, ()>);

impl ClosureGraph<'_> {
    /// Sorts all the [`ExportedPathInfo`]'s in the [`ClosureGraph`] and returns them in a [`Vec`].
    ///
    /// # Errors
    ///
    /// This function will return an error if a cycle is found in the graph.
    pub fn sort(&self) -> Result<Vec<&ExportedPathInfo>, Cycle<&ExportedPathInfo>> {
        // We use Topological Sorting to sort the graph.
        toposort(&self.0, None)
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

/// Uploads a single [`ExportedPathInfo`].
async fn upload<'a>(
    p: &'a ExportedPathInfo<'a>,
    graph: &ClosureGraph<'_>,
    uploaded: &RwLock<HashSet<&'a ExportedPathInfo<'a>>>,
) {
    // We check that all the references are already uploaded.
    let ready_for_upload = stream::iter(graph.0.neighbors_directed(p, petgraph::Incoming))
        .all(|r| async move { uploaded.read().await.contains(&r) })
        .await;

    if ready_for_upload {
        println!("UP {}", p.path);
        // TODO: Plug into an actual uploader.
        uploaded.write().await.insert(p);
    } else {
        println!("SKP {}", p.path)
    }
}

#[cfg(test)]
mod tests {
    use crate::{upload, Closure, ClosureGraph};
    use rstest::rstest;
    use std::{collections::HashSet, path::PathBuf};
    use tokio::sync::RwLock;

    #[rstest]
    #[tokio::test]
    async fn upload_all(#[files("src/fixtures/*.json")] fixture_path: PathBuf) {
        let json_data = std::fs::read(fixture_path).unwrap();
        let closure: Closure = serde_json::from_slice(&json_data).unwrap();
        let graph = ClosureGraph::from(&closure);

        let all_paths_sorted = graph.sort().unwrap();

        let uploaded = RwLock::new(HashSet::new());

        // Push tasks until all store paths have been uploaded.
        while uploaded.read().await.len() != all_paths_sorted.len() {
            let mut tasks = Vec::new();
            for path in &all_paths_sorted {
                if !uploaded.read().await.contains(path) {
                    tasks.push(upload(path, &graph, &uploaded));
                }
            }

            futures_buffered::join_all(tasks).await;
        }

        let all_paths = closure.closure.iter().collect::<HashSet<_>>();
        assert_eq!(all_paths, uploaded.into_inner());
    }

    #[rstest]
    fn all_paths_sorted(#[files("src/fixtures/*.json")] fixture_path: PathBuf) {
        let json_data = std::fs::read(fixture_path).unwrap();
        let closure: Closure = serde_json::from_slice(&json_data).unwrap();
        let graph = ClosureGraph::from(&closure);

        // These are all the store that we expect to get sorted.
        let all_paths = closure.closure.iter().collect::<HashSet<_>>();
        // We convert the `Vec`'s to `HashSet`'s because we don't care for the order of the `Vec`..
        let all_paths_sorted = graph
            .sort()
            .unwrap()
            .into_iter()
            .inspect(|p| println!("{}", p.path)) // Print each path for easier debugging.
            .collect::<HashSet<_>>();

        // We check that we didn't lose a path during the sorting.
        assert_eq!(all_paths, all_paths_sorted);
    }
}
