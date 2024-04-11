use nix_compat::store_path::StorePathRef;
use petgraph::{stable_graph::StableGraph, visit::Topo};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

#[derive(Deserialize)]
struct ClosureInner<'a> {
    #[serde(borrow)]
    #[serde(rename = "path")]
    store_path: StorePathRef<'a>,
    #[serde(borrow)]
    references: Vec<StorePathRef<'a>>,
}

#[derive(Deserialize)]
pub struct Closure<'a> {
    #[serde(borrow)]
    pub(self) closure: Vec<ClosureInner<'a>>,
}

pub struct ClosureGraph<'a>(StableGraph<StorePathRef<'a>, ()>);

impl ClosureGraph<'_> {
    /// Uploads all the [`StorePathRef`]'s in the [`ClosureGraph`] and returns them in a [`HashSet`].
    pub fn upload_all(&self) -> HashSet<StorePathRef> {
        let mut topo = Topo::new(&self.0);
        let mut uploaded = HashSet::new();
        // We use Topological Sorting to sort the graph and upload the store path as soon as it gets sorted.
        while let Some(node) = topo.next(&self.0) {
            if let Some(store_path) = self.0.node_weight(node) {
                // TODO: plug into tvix's closure uploader.
                uploaded.insert(*store_path);
                println!("Uploaded {}", store_path);
            }
        }
        uploaded
    }
}

impl<'a> From<&Closure<'a>> for ClosureGraph<'a> {
    /// Creates a new [`ClosureGraph`] from a [`Closure`]
    fn from(c: &Closure<'a>) -> Self {
        let mut graph = StableGraph::new();

        // Creates a `HashMap` binding each store path to its corresponding node index in the
        // graph. We use the store path as the node's weight/label, so this `HashMap` comes in
        // handy for looking up the node index for each store path.
        let mut node_index_map = HashMap::new();
        for c in &c.closure {
            let store_path = c.store_path;
            node_index_map.insert(store_path, graph.add_node(store_path));
        }

        // Populates the by connecting nodes (store paths) that are referenced by other nodes.
        for closure in &c.closure {
            let target_index = node_index_map.get(&closure.store_path).unwrap();
            for (store_path, source_index) in &node_index_map {
                if closure.references.contains(store_path) && target_index != source_index {
                    graph.add_edge(*source_index, *target_index, ());
                }
            }
        }

        Self(graph)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Closure, ClosureGraph};
    use nix_compat::store_path::StorePathRef;
    use rstest::rstest;
    use std::{collections::HashSet, path::PathBuf};

    #[rstest]
    fn all_references_uploaded(#[files("src/fixtures/*.json")] fixture_path: PathBuf) {
        let json_data = std::fs::read(fixture_path).unwrap();
        let closure: Closure = serde_json::from_slice(&json_data).unwrap();
        let graph = ClosureGraph::from(&closure);

        // These are all the store paths that we expect to get uploaded.
        let all_references: HashSet<StorePathRef> = closure
            .closure
            .iter()
            .flat_map(|x| x.references.clone())
            .collect();

        let uploaded = graph.upload_all();

        // We check that all the store paths indeed end up getting uploaded.
        assert_eq!(all_references, uploaded);
    }
}
