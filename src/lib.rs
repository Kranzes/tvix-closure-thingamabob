use nix_compat::path_info::ExportedPathInfo;
use petgraph::{
    algo::{toposort, Cycle},
    graphmap::DiGraphMap,
};
use serde::Deserialize;

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

#[cfg(test)]
mod tests {
    use crate::{Closure, ClosureGraph};
    use rstest::rstest;
    use std::{collections::HashSet, path::PathBuf};

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
