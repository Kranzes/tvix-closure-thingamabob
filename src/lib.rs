use async_stream::stream;
use futures_util::{Future, Stream, StreamExt};
use nix_compat::path_info::ExportedPathInfo;
use petgraph::graphmap::DiGraphMap;
use std::{collections::HashSet, ops::Deref};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    RwLock,
};
use tokio_stream::wrappers::ReceiverStream;

pub struct ClosureGraph<'a, 'data> {
    graph: RwLock<DiGraphMap<&'a ExportedPathInfo<'data>, ()>>,
    uploaded: RwLock<HashSet<&'a ExportedPathInfo<'data>>>,
    tx: Sender<ExportedPathInfo<'data>>,
    rx: Option<Receiver<ExportedPathInfo<'data>>>,
}

async fn get_ready_paths<'a, 'data: 'a, G, U>(graph: G, uploaded: U) -> Vec<ExportedPathInfo<'data>>
where
    G: Deref<Target = DiGraphMap<&'a ExportedPathInfo<'data>, ()>>,
    U: Deref<Target = HashSet<&'a ExportedPathInfo<'data>>>,
{
    graph
        .nodes()
        .filter(|&path| {
            graph
                .neighbors_directed(path, petgraph::Incoming)
                .all(|r| uploaded.contains(&r))
        })
        .map(|p| p.to_owned())
        .collect()
}

impl<'a, 'data> ClosureGraph<'a, 'data> {
    pub async fn paths_to_upload(&'a mut self) -> impl Stream<Item = ExportedPathInfo> + 'data
    where
        'a: 'data,
    {
        //let ready_paths =
        //    get_ready_paths(self.graph.read().await, self.uploaded.read().await).await;
        //let tx = self.tx.take().unwrap();
        //tokio::spawn(async move {
        //    for path in ready_paths {
        //        tx.send(path).await.unwrap()
        //    }
        //});
        let graph = self.graph.read().await;
        let uploaded = self.uploaded.read().await;
        stream! {
            let ready_for_upload = {
                graph.nodes().filter(|&path| {
                  graph
                      .neighbors_directed(path, petgraph::Incoming)
                      .all(|r| uploaded.contains(&r))
                }).map(|p| p.to_owned()).collect::<Vec<_>>()
            };

            for path in ready_for_upload {
               yield path;
            }
        }
        .chain(ReceiverStream::new(self.rx.take().unwrap()))
        //stream! {
        //    loop {
        //        let graph = self.graph.read().await;
        //
        //        if graph.node_count() == 0 {
        //            break;
        //        }
        //
        //        let ready_for_upload = {
        //            let uploaded = self.uploaded.read().await;
        //            graph.nodes().filter(|&path| {
        //              graph
        //                  .neighbors_directed(path, petgraph::Incoming)
        //                  .all(|r| uploaded.contains(&r))
        //            }).collect::<Vec<_>>()
        //        };
        //
        //        for path in ready_for_upload {
        //           yield path;
        //        }
        //    }
        //}
    }

    pub async fn mark_uploaded(&self, path: &'a ExportedPathInfo<'data>) {
        self.uploaded.write().await.insert(path);
        self.graph.write().await.remove_node(path);
        for p in get_ready_paths(self.graph.read().await, self.uploaded.read().await).await {
            self.tx.send(p).await.unwrap();
        }
    }

    pub fn from_exported_pathinfos(
        exported_pathinfos: &'a [ExportedPathInfo<'data>],
    ) -> ClosureGraph<'a, 'data>
//where
    //    'b: 'a,
    {
        // Create edges from nodes (exported path infos) that are referenced by other nodes.
        let edges = exported_pathinfos.iter().flat_map(|target_path| {
            exported_pathinfos
                .iter()
                .filter(|source_path| {
                    source_path.path != target_path.path // Avoid self references.
                    && target_path.references.contains(&source_path.path)
                })
                .map(move |source_path| (source_path, target_path))
        });

        let (tx, rx) = tokio::sync::mpsc::channel(10);

        Self {
            graph: RwLock::new(DiGraphMap::from_edges(edges)),
            uploaded: RwLock::new(HashSet::new()),
            tx,
            rx: Some(rx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{pin_mut, StreamExt};
    use rand::Rng;
    use rstest::rstest;
    use serde::Deserialize;
    use std::path::PathBuf;
    use tokio::{
        task::JoinSet,
        time::{sleep, Duration},
    };

    #[derive(Deserialize)]
    struct Closure<'a> {
        #[serde(borrow)]
        closure: Vec<ExportedPathInfo<'a>>,
    }

    #[rstest]
    #[tokio::test]
    async fn upload_all(#[files("src/fixtures/*.json")] fixture_path: PathBuf) {
        let json_data = tokio::fs::read(fixture_path).await.unwrap();
        let c: Closure = serde_json::from_slice(&json_data).unwrap();
        let mut graph = ClosureGraph::from_exported_pathinfos(&c.closure);

        //let mut set = JoinSet::new();
        let stream = graph.paths_to_upload();
        //pin_mut!(stream);

        //stream
        //    .await
        //    .for_each_concurrent(None, |path| async move {
        //        sleep(Duration::from_secs(rand::thread_rng().gen_range(0..2))).await;
        //        graph.mark_uploaded(&path).await;
        //        println!("{}", path.path);
        //    })
        //    .await;

        //while let Some(path) = stream.await.next().await {
        //    graph.mark_uploaded(&path);
        //    println!("{}", path.path);
        //}

        //if let Some((path, mark_uploaded)) = stream.next().await {
        //    set.spawn(async move {
        //        println!("Attempting to uploaded {}", path.path);
        //        sleep(Duration::from_secs(rand::thread_rng().gen_range(0..2))); // mimics uploading
        //        (path, mark_uploaded)
        //    });
        //}
        //
        //while let Some(res) = set.join_next().await {
        //    let (path, mark_uploaded) = res.unwrap();
        //    mark_uploaded();
        //    println!("Uploaded {}", path.path);
        //}

        //let all_paths = c.closure.iter().collect::<HashSet<_>>();
        //assert_eq!(all_paths, *graph.uploaded.read().await);
    }
}
