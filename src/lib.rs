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

impl Closure<'_> {
    pub fn to_graph(&self) -> Result<StableGraph<StorePathRef, ()>, Box<dyn std::error::Error>> {
        let mut graph = StableGraph::new();

        let node_index_map: HashMap<_, _> = self
            .closure
            .iter()
            .map(|closure| (closure.store_path, graph.add_node(closure.store_path)))
            .collect();

        for closure in &self.closure {
            for (node, source_index) in &node_index_map {
                if closure.references.contains(node) {
                    if let Some(target_index) = node_index_map.get(&closure.store_path) {
                        if target_index != source_index {
                            graph.add_edge(*source_index, *target_index, ());
                        }
                    }
                }
            }
        }

        Ok(graph)
    }
}

pub fn do_work(graph: StableGraph<StorePathRef, ()>) -> HashSet<StorePathRef> {
    let mut topo = Topo::new(&graph);
    let mut uploaded = HashSet::new();
    while let Some(node) = topo.next(&graph) {
        if let Some(store_path) = graph.node_weight(node) {
            uploaded.insert(*store_path);
            println!("Uploaded {}", store_path);
        }
    }
    uploaded
}

#[cfg(test)]
mod tests {
    use crate::{do_work, Closure};
    use nix_compat::store_path::StorePathRef;
    use rstest::rstest;
    use std::{collections::HashSet, path::PathBuf};

    #[rstest]
    fn all_references_uploaded(#[files("src/fixtures/*.json")] fixture_path: PathBuf) {
        let json_data = std::fs::read(fixture_path).unwrap();
        let closure: Closure = serde_json::from_slice(&json_data).unwrap();
        let graph = closure.to_graph().unwrap();

        let all_references: HashSet<StorePathRef> = closure
            .closure
            .iter()
            .flat_map(|x| x.references.clone())
            .collect();

        let uploaded = do_work(graph);

        assert_eq!(all_references, uploaded);
    }
}
