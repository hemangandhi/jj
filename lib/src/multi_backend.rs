// Copyright 2020 The Jujutsu Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![expect(missing_docs)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error as FmtError;
use std::fmt::Formatter;
use std::pin::Pin;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::stream::BoxStream;
use prost::Message;
use tokio::io::AsyncRead;

use crate::backend::Backend;
use crate::backend::BackendError;
use crate::backend::BackendInitError;
use crate::backend::BackendLoadError;
use crate::backend::BackendResult;
use crate::backend::ChangeId;
use crate::backend::Commit;
use crate::backend::CommitId;
use crate::backend::CopyHistory;
use crate::backend::CopyId;
use crate::backend::CopyRecord;
use crate::backend::FileId;
use crate::backend::MillisSinceEpoch;
use crate::backend::SecureSig;
use crate::backend::Signature;
use crate::backend::SigningFn;
use crate::backend::SymlinkId;
use crate::backend::Timestamp;
use crate::backend::Tree;
use crate::backend::TreeId;
use crate::backend::TreeValue;
use crate::backend::make_root_commit;
use crate::content_hash::ContentHash;
use crate::index::Index;
use crate::merge::Merge;
use crate::object_id::ObjectId;
use crate::protos::multi_backend_commit::Commit as MultiBackendCommit;
use crate::protos::multi_backend_tree_ids::TreeIds as MultiBackendTreeIds;
use crate::repo_path::RepoPath;
use crate::repo_path::RepoPathBuf;
use crate::repo_path::RepoPathComponent;
use crate::repo_path::RepoPathComponentBuf;
use crate::repo_path::RepoPathTree;
use crate::stacked_table::TableSegment;
use crate::stacked_table::TableStore;
use crate::stacked_table::TableStoreResult;

#[derive(Debug)]
struct FileNotFoundError {}

impl Display for FileNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "File not found")
    }
}

impl Error for FileNotFoundError {}

// int64 in proto?
const HASH_LENGTH: usize = 8;

pub struct MultiBackend {
    backends: RepoPathTree<Option<Box<dyn Backend>>>,
    /// Each value is a vector clock where each element is a backend.
    /// The whole table is actually a record of all the vector values that ever
    /// existed with some index for this backend to be able to also identify
    /// the vectors. Also, the components of the vector are indexed by the
    /// path so that we don't have to memorize some order of inner backends.
    // A table of multi_backend_commit::Commit protos.
    commit_clock: TableStore,
    trees: TableStore,
    root_commit_id: CommitId,
    root_tree_id: TreeId,
    root_change_id: ChangeId,
}

impl Debug for MultiBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        f.debug_struct("MultiBackend")
            .field("members", &self.backends)
            .finish()
    }
}

impl MultiBackend {
    pub fn new(
        backends: RepoPathTree<Option<Box<dyn Backend>>>,
        commit_clock: TableStore,
        trees: TableStore,
    ) -> Self {
        Self {
            backends,
            commit_clock,
            trees,
            root_commit_id: CommitId::from_bytes(&[0; HASH_LENGTH]),
            root_tree_id: TreeId::from_bytes(&[0; 8]),
            root_change_id: ChangeId::from_bytes(&[0; 8]),
        }
    }

    fn read_table_at_commit(&self, id: &CommitId) -> TableStoreResult<Option<MultiBackendCommit>> {
        let (table, _lock) = self.commit_clock.get_head_locked()?;
        match table.get_value(id.as_bytes()) {
            Some(serialized_proto) => Ok(Some(
                MultiBackendCommit::decode(serialized_proto)
                    .expect("Couldn't deserialize multi-backend commit."),
            )),
            None => return Ok(None),
        }
    }

    fn get_backend_at_path<'a, 'b>(
        &'a self,
        path: &'b RepoPath,
    ) -> Option<(&'a Box<dyn Backend>, &'b RepoPath)> {
        let (tree, rest_of_path) = self.backends.walk_to(path).last()?;
        if let Some(boxed_backend) = tree.value().as_ref() {
            Some((boxed_backend, rest_of_path))
        } else {
            None
        }
    }
}

#[async_trait]
impl Backend for MultiBackend {
    fn name(&self) -> &str {
        "multi-backend"
    }

    fn commit_id_length(&self) -> usize {
        HASH_LENGTH
    }

    fn change_id_length(&self) -> usize {
        HASH_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.root_tree_id
    }

    fn concurrency(&self) -> usize {
        self.backends
            .values()
            .iter()
            .map(|b| {
                b.as_ref()
                    .map(|has_b| has_b.concurrency())
                    .unwrap_or(0usize)
            })
            .sum()
    }

    async fn read_file(
        &self,
        path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let (backend, rest_of_path) =
            self.get_backend_at_path(path)
                .ok_or_else(|| BackendError::ReadFile {
                    path: path.to_owned(),
                    id: id.clone(),
                    source: Box::new(FileNotFoundError {}),
                })?;
        backend.read_file(rest_of_path, id).await
    }

    async fn write_file(
        &self,
        path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let (backend, rest_of_path) =
            self.get_backend_at_path(path)
                .ok_or_else(|| BackendError::WriteObject {
                    object_type: "File",
                    source: Box::new(FileNotFoundError {}),
                })?;
        backend.write_file(rest_of_path, contents).await
    }

    async fn read_symlink(&self, path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        Err(BackendError::Unsupported(
            "Symlinking across backends is not supported.".to_string(),
        ))
    }

    async fn write_symlink(&self, path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        Err(BackendError::Unsupported(
            "Symlinking across backends is not supported.".to_string(),
        ))
    }

    async fn read_copy(&self, id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "The multi-backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn write_copy(&self, copy: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "The multi-backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn get_related_copies(&self, copy_id: &CopyId) -> BackendResult<Vec<CopyHistory>> {
        Err(BackendError::Unsupported(
            "The multi-backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        let paths_and_trees = {
            let (table, _lock) =
                self.trees
                    .get_head_locked()
                    .map_err(|err| BackendError::ReadObject {
                        object_type: id.object_type(),
                        hash: id.to_string(),
                        source: Box::new(err),
                    })?;
            table
                .get_value(id.as_bytes())
                .map(|v| MultiBackendTreeIds::decode(v).expect("Couldn't parse proto."))
                .ok_or_else(|| BackendError::ReadObject {
                    object_type: id.object_type(),
                    hash: id.to_string(),
                    source: Box::new(FileNotFoundError {}),
                })?
        };

        let mut tree_entries = Vec::new();
        for inner_tree in paths_and_trees.inner_trees {
            let repo_path_buf = RepoPathBuf::from_relative_path(inner_tree.path.clone())
                .expect("Couldn't parse repo path in proto");
            let (backend, rest_of_path) = self
                .get_backend_at_path(repo_path_buf.as_ref())
                .ok_or_else(|| BackendError::ReadObject {
                    object_type: "Tree".to_string(),
                    hash: "".to_string(),
                    source: Box::new(FileNotFoundError {}),
                })?;
            let sub_tree = backend.read_tree(rest_of_path, id).await?;
            tree_entries.extend(sub_tree.entries().map(|tree_entry| {
                (
                    RepoPathComponentBuf::new(
                        repo_path_buf
                            .as_ref()
                            .join(tree_entry.name())
                            .as_ref()
                            .as_internal_file_string(),
                    )
                    .expect("How is path invalid?"),
                    tree_entry.value().clone(),
                )
            }));
        }
        tree_entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        Ok(Tree::from_sorted_entries(tree_entries))
    }

    async fn write_tree(&self, path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        let (backend, rest_of_path) =
            self.get_backend_at_path(path)
                .ok_or_else(|| BackendError::WriteObject {
                    object_type: "Tree",
                    source: Box::new(FileNotFoundError {}),
                })?;
        backend.write_tree(rest_of_path, contents).await
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        let commit_proto = self
            .read_table_at_commit(id)
            .map_err(|table_err| BackendError::ReadObject {
                object_type: id.object_type(),
                hash: id.to_string(),
                source: Box::new(table_err),
            })?
            .ok_or(BackendError::ReadObject {
                object_type: id.object_type(),
                hash: id.to_string(),
                source: Box::new(FileNotFoundError {}),
            })?;

        // NOTE: I think being able to `.collect` an iterator would be faster, the
        // `.await` deep in a closure scares me and I think the lifetimes will
        // be hard to manage.
        let mut inner_commits: HashMap<String, (&Box<dyn Backend>, Commit)> =
            HashMap::with_capacity(commit_proto.inner_backend_commits.len());
        for inner_backend_commit in commit_proto.inner_backend_commits {
            let repo_path_buf = RepoPathBuf::from_relative_path(inner_backend_commit.path.clone())
                .expect("Couldn't parse repo path in proto");
            let inner_id = CommitId::from_bytes(&inner_backend_commit.commit_id);
            let (backend, inner_commit) =
                if let Some((backend, _path)) = self.get_backend_at_path(repo_path_buf.as_ref()) {
                    (backend, backend.read_commit(&inner_id).await?)
                } else {
                    return Err(BackendError::ReadObject {
                        object_type: id.object_type(),
                        hash: id.to_string(),
                        source: Box::new(FileNotFoundError {}),
                    });
                };
            inner_commits.insert(inner_backend_commit.path.clone(), (backend, inner_commit));
        }

        if inner_commits.is_empty() {
            return Err(BackendError::ObjectNotFound {
                object_type: id.object_type(),
                hash: id.to_string(),
                source: Box::new(FileNotFoundError {}),
            });
        }

        Ok(Commit {
            parents: commit_proto
                .parent_ids
                .iter()
                .map(|i| CommitId::from_bytes(&i.to_le_bytes()))
                .collect(),
            predecessors: commit_proto
                .predecessor_ids
                .iter()
                .map(|i| CommitId::from_bytes(&i.to_le_bytes()))
                .collect(),
            root_tree: Merge::from_vec(
                commit_proto
                    .merge_data
                    .iter()
                    .map(|datum| TreeId::from_bytes(&datum.tree_id.to_le_bytes()))
                    .collect::<Vec<TreeId>>(),
            ),
            conflict_labels: Merge::from_vec(
                commit_proto
                    .merge_data
                    .iter()
                    .map(|datum| datum.conflict_label.clone())
                    .collect::<Vec<String>>(),
            ),
            change_id: ChangeId::from_bytes(&commit_proto.change_id.to_le_bytes()),
            description: commit_proto.description,
            author: Signature {
                name: commit_proto.author_name,
                email: commit_proto.author_email,
                timestamp: Timestamp {
                    timestamp: MillisSinceEpoch(
                        commit_proto
                            .author_timestamp
                            .map(|t| t.millis_since_epoch)
                            .unwrap_or(0),
                    ),
                    tz_offset: commit_proto
                        .author_timestamp
                        .map(|t| t.tz_offset)
                        .unwrap_or(0),
                },
            },
            committer: Signature {
                name: commit_proto.committer_name,
                email: commit_proto.committer_email,
                timestamp: Timestamp {
                    timestamp: MillisSinceEpoch(
                        commit_proto
                            .committer_timestamp
                            .map(|t| t.millis_since_epoch)
                            .unwrap_or(0),
                    ),
                    tz_offset: commit_proto
                        .committer_timestamp
                        .map(|t| t.tz_offset)
                        .unwrap_or(0),
                },
            },
            secure_sig: None, // TODO: plonk these into the proto too?
        })
    }

    async fn write_commit(
        &self,
        contents: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        todo!()
    }

    fn get_copy_records(
        &self,
        paths: Option<&[RepoPathBuf]>,
        root: &CommitId,
        head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        todo!()
    }

    fn gc(&self, index: &dyn Index, keep_newer: SystemTime) -> BackendResult<()> {
        todo!()
    }
}
