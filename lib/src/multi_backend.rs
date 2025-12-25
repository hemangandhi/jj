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
use std::collections::hash_map::Entry;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error as FmtError;
use std::fmt::Formatter;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::StreamExt;
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
use crate::backend::Signature;
use crate::backend::SigningFn;
use crate::backend::SymlinkId;
use crate::backend::Timestamp;
use crate::backend::Tree;
use crate::backend::TreeId;
use crate::backend::TreeValue;
use crate::content_hash::blake2b_hash;
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
use crate::settings::UserSettings;
use crate::stacked_table::TableSegment;
use crate::stacked_table::TableStore;
use crate::stacked_table::TableStoreResult;

// TODO: Enumerate some actual errors here.
#[derive(Debug)]
struct MultiBackendError(String);

impl Display for MultiBackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Multi-backend error: {}", self.0)
    }
}

impl Error for MultiBackendError {}

// TODO: unify everything to the below; using 512-bit hashes.
// (Recall 512 bits = 64 bytes * 8 bits / byte)
const HASH_LENGTH: usize = 64;

const COMMIT_STORE_DIR: &str = "multi_backend_commits";
const TREE_STORE_DIR: &str = "multi_backend_trees";
const PATHS_STORE_FILE: &str = "multi_backend_members.pb";


struct BackendAndCommits<'a> {
    backend: &'a Arc<dyn Backend>,
    root: CommitId,
    head: CommitId,
    paths: Option<Vec<RepoPathBuf>>
}

impl<'a> BackendAndCommits<'a> {
    fn from_root_commit(
        backend: &'a Arc<dyn Backend>,
        root: &Vec<u8>,
        paths: Option<&[RepoPathBuf]>,
        path_prefix: &RepoPath
    ) -> BackendAndCommits<'a> {
         BackendAndCommits {
            backend: backend,
            root: CommitId::from_bytes(root.as_ref()),
            // TODO: does this make any sense? Is Option<CommitId> better here?
            head: backend.root_commit_id().clone(),
            paths: paths.map(|ps| {
                ps.iter()
                    .filter_map(|p| p.strip_prefix(&path_prefix))
                    .map(|p| p.to_owned())
                    .collect()
            })
        }
    }

    fn from_head_commit(
        backend: &'a Arc<dyn Backend>,
        head: &Vec<u8>,
        paths: Option<&[RepoPathBuf]>,
        path_prefix: &RepoPath
    ) -> BackendAndCommits<'a> {
         BackendAndCommits {
            backend: backend,
            root: backend.root_commit_id().clone(),
            head: CommitId::from_bytes(head.as_ref()),
            paths: paths.map(|ps| {
                ps.iter()
                    .filter_map(|p| p.strip_prefix(&path_prefix))
                    .map(|p| p.to_owned())
                    .collect()
            })
        }
    }

    fn get_copy_records(
        self,
        path_prefix: RepoPathBuf,
        head: CommitId,
        root: CommitId
    ) -> BackendResult<BoxStream<'a, BackendResult<CopyRecord>>> {
        self
            .backend
            .get_copy_records(
                self.paths.as_deref(),
                &self.root,
                &self.head
            )
            .map(move |stream: BoxStream<'_, BackendResult<CopyRecord>>| {
                stream
                    .map(move |maybe_copy: BackendResult<CopyRecord>| {
                        maybe_copy.map(|copy| CopyRecord {
                            target: path_prefix.concat(&copy.target),
                            target_commit: head.clone(),
                            source: path_prefix.concat(&copy.source),
                            source_file: copy.source_file,
                            source_commit: root.clone()
                        })
                    })
            })
            .map(move |ok| -> BoxStream<'_, BackendResult<CopyRecord>> {Box::pin(ok)})
    }
}

pub struct MultiBackend {
    // TODO: this is only an Arc to enable us to clone the input when making a repo.
    // See if we can actually have unique ownership with better APIs.
    backends: RepoPathTree<Option<Arc<dyn Backend>>>,
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
    fn new(
        backends: RepoPathTree<Option<Arc<dyn Backend>>>,
        commit_clock: TableStore,
        trees: TableStore,
    ) -> Self {
        Self {
            backends,
            commit_clock,
            trees,
            root_commit_id: CommitId::from_bytes(&[0; HASH_LENGTH]),
            root_tree_id: TreeId::from_bytes(&[0; HASH_LENGTH]),
            root_change_id: ChangeId::from_bytes(&[0; HASH_LENGTH]),
        }
    }

    fn write_root_commit(&self) -> Result<(), BackendInitError> {
        let root_commit = MultiBackendCommit {
            id: [0; HASH_LENGTH].to_vec(),
            merge_data: vec![crate::protos::multi_backend_commit::LabeledMergeTrees {
                tree_id: self.root_tree_id.to_bytes(),
                conflict_label: "".to_owned(),
            }],
            change_id: [0; HASH_LENGTH].to_vec(),
            inner_backend_commits: self
                .backends
                .get_flat_path_map(&RepoPathBuf::root())
                .iter()
                .filter_map(|(path, maybe_backend)| {
                    maybe_backend.as_ref().map(|backend| {
                        crate::protos::multi_backend_commit::InnerBackendCommit {
                            commit_id: backend.root_commit_id().to_bytes().to_vec(),
                            path: path.as_internal_file_string().to_string(),
                            deleted: false,
                        }
                    })
                })
                .collect(),
            ..MultiBackendCommit::default()
        };

        let (table, _lock) = self
            .commit_clock
            .get_head_locked()
            .map_err(|e| BackendInitError(Box::new(e)))?;
        let mut mut_table = table.start_mutation();
        mut_table.add_entry([0; HASH_LENGTH].to_vec(), root_commit.encode_to_vec());
        self.commit_clock
            .save_table(mut_table)
            .map_err(|e| BackendInitError(Box::new(e)))?;
        Ok(())
    }

    fn read_tree_proto(&self, path: &RepoPath, id: &TreeId) -> BackendResult<MultiBackendTreeIds> {
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
                source: Box::new(MultiBackendError(format!(
                    "Couldn't find tree {} for values under path {:?}",
                    id.to_string(),
                    path
                ))),
            })
    }

    /// Used for writing trees, as a check that an inner tree value is actually
    /// from this backend instead of one of the inner ones.
    fn is_tree_at_path_multi_backend(&self, path: &RepoPath, id: &TreeId) -> bool {
         if self.get_backend_at_path(path).is_none() {
            return true;
        }
        self.read_tree_proto(path, id).is_ok()
    }

    fn write_multi_backend_tree(&self, path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        let mut tree_proto: MultiBackendTreeIds = MultiBackendTreeIds {
            id: blake2b_hash(&(path.to_owned(), contents.clone())).to_vec(),
            inner_trees: vec![],
            root_path: path.to_owned().as_internal_file_string().to_string(),
        };
        for tree_entry in contents.entries() {
            if let TreeValue::Tree(inner_id) = tree_entry.value() {
                tree_proto.inner_trees.push(
                    crate::protos::multi_backend_tree_ids::InnerBackendTreeId {
                        tree_id: inner_id.to_bytes(),
                        path: tree_entry.name().as_internal_str().to_owned(),
                        is_tree_multi_backend: self.is_tree_at_path_multi_backend(
                            path.join(tree_entry.name()).as_ref(),
                            inner_id
                        )
                    },
                )
            } else {
                return Err(BackendError::Unsupported(
                    "Multi-commit backend was not merely given some tree. What's going on?"
                        .to_owned(),
                ));
            }
        }

        let output_tree_id = TreeId::from_bytes(&tree_proto.id);
        let encoded_tree = tree_proto.encode_to_vec();
        let (table, _lock) =
            self.trees
                .get_head_locked()
                .map_err(|err| BackendError::WriteObject {
                    object_type: "tree",
                    source: Box::new(err),
                })?;
        let mut mut_table = table.start_mutation();
        mut_table.add_entry(tree_proto.id, encoded_tree);
        self.trees
            .save_table(mut_table)
            .map_err(|err| BackendError::WriteObject {
                object_type: "tree",
                source: Box::new(err),
            })?;
        return Ok(output_tree_id);
    }

    /// Traverse all the backends as a DFS to create the empty tree for the multi-backend.
    /// The multi-backend's empty tree is the tree with all the empty trees of the inner backends.
    fn traverse_backends_for_empty_tree(
        &self,
        parent_bit: &RepoPathComponentBuf,
        path: &RepoPath,
        layer: &RepoPathTree<Option<Arc<dyn Backend>>>,
    ) -> Result<TreeValue, BackendInitError> {
        // println!("Traversing under {:#?} with parent {:#?}", path, parent_bit);
        let mut layer_entries  = layer.children().map(|(path_bit, sub_tree)| {
            let owned_bit = path_bit.to_owned();
            let inner_value = self.traverse_backends_for_empty_tree(&owned_bit, &path.join(path_bit), sub_tree)?;
            Ok((owned_bit, inner_value))
        }).collect::<Result<Vec<(RepoPathComponentBuf, TreeValue)>, BackendInitError>>()?;
        if let Some(inner_backend) = layer.value() {
            layer_entries.push((parent_bit.clone(), TreeValue::Tree(inner_backend.empty_tree_id().clone())));
        }
        layer_entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        // println!("Path {:#?} Layer entries? {:#?}", path, layer_entries);
        let tree = Tree::from_sorted_entries(layer_entries);
        let tree_id = self.write_multi_backend_tree(path.parent().unwrap_or(&RepoPath::root()), &tree).map_err(|e| BackendInitError(Box::new(e)))?;
        Ok(TreeValue::Tree(tree_id))
    }

    fn write_root_tree(&mut self) -> Result<(), BackendInitError> {
        let mut top_level_children = self.backends.children().map(|(bit, sub_tree)| {
            let owned_bit = bit.to_owned();
            let inner_tree = self.traverse_backends_for_empty_tree(&owned_bit, &RepoPath::root().join(bit), sub_tree)?;
            Ok((owned_bit, inner_tree))
        }).collect::<Result<Vec<(RepoPathComponentBuf, TreeValue)>, BackendInitError>>()?;

        top_level_children.sort_by(|(a, _), (b, _)| a.cmp(b));
        let tree = Tree::from_sorted_entries(top_level_children);
        self.root_tree_id = self.write_multi_backend_tree(&RepoPath::root(), &tree).map_err(|e| BackendInitError(Box::new(e)))?;
        Ok(())
    }

    fn write_path_store_file(
        &self, // TODO: keep this &self to enforce ordering or omit?
        store_path: &Path,
        path_map: HashMap<String, String>,
    ) -> Result<(), BackendInitError> {
        let proto = crate::protos::multi_backend_paths::MultiBackendPaths {
            repo_to_store_path_map: path_map,
        };
        std::fs::write(store_path.join(PATHS_STORE_FILE), proto.encode_to_vec())
            .map_err(|e| BackendInitError(Box::new(e)))
    }

    fn create_stores_from_path(
        backends: RepoPathTree<Option<Arc<dyn Backend>>>,
        store_path: &Path,
        create_dirs: bool,
    ) -> std::io::Result<Self> {
        let commit_path = store_path.join(COMMIT_STORE_DIR);
        let tree_path = store_path.join(TREE_STORE_DIR);
        if create_dirs {
            std::fs::create_dir(&commit_path)?;
            std::fs::create_dir(&tree_path)?;
            Ok(MultiBackend::new(
                backends,
                TableStore::init(commit_path, HASH_LENGTH),
                TableStore::init(tree_path, HASH_LENGTH),
            ))
        } else {
            Ok(MultiBackend::new(
                backends,
                TableStore::load(commit_path, HASH_LENGTH),
                TableStore::load(tree_path, HASH_LENGTH),
            ))
        }
    }

    fn init_from_path(
        backends: RepoPathTree<Option<Arc<dyn Backend>>>,
        store_path: &Path,
    ) -> Result<Self, BackendInitError> {
        let mut uninitialized_backend =
            MultiBackend::create_stores_from_path(backends, store_path, true)
                .map_err(|e| BackendInitError(Box::new(e)))?;

        uninitialized_backend.write_root_tree()?;
        uninitialized_backend.write_root_commit()?;
        Ok(uninitialized_backend)
    }

    pub fn init_with_backends_at_paths(
        settings: &UserSettings,
        repo_paths: &HashSet<RepoPathBuf>,
        store_path: &Path,
        backend_creator: &impl Fn(
            &UserSettings,
            &Path, /* repo path */
            &Path, /* store path */
        ) -> Result<Arc<dyn Backend>, BackendInitError>,
    ) -> Result<Self, BackendInitError> {
        let mut tree: RepoPathTree<Option<Arc<dyn Backend>>> = RepoPathTree::default();
        let mut path_map: HashMap<String, String> = HashMap::with_capacity(repo_paths.len());
        for (i, inner_repo_path) in repo_paths.iter().enumerate() {
            let inner_store_dir = store_path.join(format!("repo{}", i));
            std::fs::create_dir_all(&inner_store_dir).map_err(|e| BackendInitError(Box::new(e)))?;
            let std_repo_path: std::path::PathBuf =
                inner_repo_path.to_fs_path_unchecked(Path::new("")).into();
            if !inner_store_dir.exists() {
                std::fs::create_dir(&inner_store_dir).map_err(|e| BackendInitError(Box::new(e)))?;
            }
            let inner_backend =
                backend_creator(settings, std_repo_path.as_path(), &inner_store_dir)?;
            std::fs::write(inner_store_dir.join("type"), inner_backend.name()).map_err(|e| BackendInitError(Box::new(e)))?;
            tree.add(inner_repo_path).set_value(Some(inner_backend));
            path_map.insert(
                inner_repo_path.as_internal_file_string().to_string(),
                inner_store_dir.display().to_string(),
            );
        }
        let backend = Self::init_from_path(tree, store_path)?;
        backend.write_path_store_file(store_path, path_map)?;
        Ok(backend)
    }

    pub fn load(
        settings: &UserSettings,
        store_path: &Path,
        backend_loader: &impl Fn(&UserSettings, &Path) -> Result<Arc<dyn Backend>, BackendLoadError>
    ) -> Result<Self, BackendLoadError> {
        let path_map_contents = std::fs::read(store_path.join(PATHS_STORE_FILE))
            .map_err(|e| BackendLoadError(Box::new(e)))?;
        let path_map =
            crate::protos::multi_backend_paths::MultiBackendPaths::decode(&*path_map_contents)
                .map_err(|e| BackendLoadError(Box::new(e)))?;
        let mut loaded_tree: RepoPathTree<Option<Arc<dyn Backend>>> = RepoPathTree::default();
        for (repo_path, inner_store_path) in path_map.repo_to_store_path_map {
            let fs_path: std::path::PathBuf = inner_store_path.into();
            let loaded_inner_backend = backend_loader(settings, fs_path.as_ref())?;
            loaded_tree
                .add(
                    RepoPathBuf::from_internal_string(repo_path)
                        .expect("Failed to parse repo path while reloading backend.")
                        .as_ref(),
                )
                .set_value(Some(loaded_inner_backend));
        }

        MultiBackend::create_stores_from_path(loaded_tree, store_path, false)
            .map_err(|e| BackendLoadError(Box::new(e)))
    }

    pub fn name() -> &'static str {
        "multi-backend"
    }

    fn read_table_at_commit(&self, id: &CommitId) -> TableStoreResult<Option<MultiBackendCommit>> {
        let (table, _lock) = self.commit_clock.get_head_locked()?;
        match table.get_value(id.as_bytes()) {
            Some(serialized_proto) => Ok(Some(
                MultiBackendCommit::decode(serialized_proto)
                    .expect("Couldn't deserialize multi-backend commit."),
            )),
            None => Ok(None),
        }
    }

    fn read_table_at_treeid(&self, id: &TreeId) -> TableStoreResult<Option<MultiBackendTreeIds>> {
        let (table, _lock) = self.trees.get_head_locked()?;
        match table.get_value(id.as_bytes()) {
            Some(proto) => Ok(Some(
                MultiBackendTreeIds::decode(proto).expect("Couldn't deserialize tree ID."),
            )),
            None => Ok(None),
        }
    }

    fn get_backend_at_path<'a, 'b>(
        &'a self,
        path: &'b RepoPath,
    ) -> Option<(&'a Arc<dyn Backend>, &'b RepoPath)> {
        let (tree, rest_of_path) = self.backends.walk_to(path).last()?;
        if let Some(boxed_backend) = tree.value().as_ref() {
            Some((boxed_backend, rest_of_path))
        } else {
            None
        }
    }

    /// Get groups of commits that pertain to a backend.
    /// The input commits are assumed to be multi-backend commits (parents, for
    /// example), and each of their inner backend commit IDs are grouped.
    ///
    /// Used to write the appropriate commit to the appropriate inner backend.
    fn group_backends_by_commit_ids<'a>(
        &self,
        commit_ids: impl Iterator<Item = &'a CommitId>,
    ) -> BackendResult<HashMap<RepoPathBuf, Vec<CommitId>>> {
        let mut groups: HashMap<RepoPathBuf, Vec<CommitId>> = HashMap::new();
        for id in commit_ids {
            let commit = self
                .read_table_at_commit(&id)
                .map_err(|err| BackendError::ReadObject {
                    object_type: id.object_type(),
                    hash: id.to_string(),
                    source: Box::new(err),
                })?
                .ok_or_else(|| BackendError::ReadObject {
                    object_type: id.object_type(),
                    hash: id.to_string(),
                    source: Box::new(MultiBackendError(format!(
                        "Failed to read commit {}",
                        id.to_string()
                    ))),
                })?;
            for inner_commit in commit.inner_backend_commits {
                groups
                    .entry(
                        RepoPath::from_internal_string(&inner_commit.path)
                            .expect("Couldn't parse commit backend path")
                            .to_owned(),
                    )
                    .or_insert(vec![])
                    .push(CommitId::from_bytes(&inner_commit.commit_id));
            }
        }
        Ok(groups)
    }

    /// Get the tress inside all the backends.
    /// Maps from paths to the trees for the backend that should be at that path
    /// and the conflict labels per tree.
    // Implementation note: this is a BFS for simplicity of keeping all the data in
    // one place.
    fn get_trees_inside_backends<'a>(
        &self,
        merges_and_labels: &mut impl Iterator<Item = (&'a TreeId, &'a String)>,
    ) -> BackendResult<HashMap<RepoPathBuf, Vec<(TreeId, String)>>> {
        let mut merge_conflict_per_path: HashMap<RepoPathBuf, Vec<(TreeId, String)>> =
            HashMap::new();
        let mut trees_to_traverse_under: HashMap<(TreeId, String), String> = HashMap::new();
        // TODO: consolidate this into the BFS loop? Or is the optimization for shallow
        // workspaces worth it?
        for (merge_id, label) in merges_and_labels {
            let tree = self
                .read_table_at_treeid(merge_id)
                .map_err(|err| BackendError::ReadObject {
                    object_type: merge_id.object_type(),
                    hash: merge_id.to_string(),
                    source: Box::new(err),
                })?
                .ok_or_else(|| BackendError::ReadObject {
                    object_type: merge_id.object_type(),
                    hash: merge_id.to_string(),
                    source: Box::new(MultiBackendError(format!(
                        "Failed to read tree {} for a commit.",
                        merge_id.to_string()
                    ))),
                })?;
            let root_path = RepoPathBuf::from_internal_string(tree.root_path).map_err(|err| {
                BackendError::ReadObject {
                    object_type: merge_id.object_type(),
                    hash: merge_id.to_string(),
                    source: Box::new(err),
                }
            })?;
            for sub_tree in tree.inner_trees {
                let sub_tree_id = TreeId::from_bytes(&sub_tree.tree_id);
                let total_path = root_path
                    .join(RepoPathComponent::new(&sub_tree.path).expect("Couldn't parse tree path"))
                    .to_owned();
                if !sub_tree.is_tree_multi_backend {
                    merge_conflict_per_path
                        .entry(total_path)
                        .or_insert(vec![])
                        .push((sub_tree_id, label.to_string()))
                } else {
                    let map_entry = trees_to_traverse_under.entry((sub_tree_id.clone(), total_path.as_internal_file_string().to_owned()));
                    if let Entry::Occupied(_occ) = map_entry {
                        return Err(BackendError::Other(Box::new(MultiBackendError(format!(
                            "Colliding tree ids and paths, directly in the merge labels! {} duplicated, duplicate is at {:?}",
                            sub_tree_id.to_string(),
                            total_path
                        )))));
                    }
                    map_entry.or_insert(label.to_string());
                }
            }
        }

        // Main BFS loop.
        while !trees_to_traverse_under.is_empty() {
            let mut next_trees_to_traverse_under: HashMap<(TreeId, String), String> = HashMap::new();
            for ((tree_id, _path), label) in trees_to_traverse_under.drain() {
                let tree = self
                    .read_table_at_treeid(&tree_id)
                    .map_err(|err| BackendError::ReadObject {
                        object_type: tree_id.object_type(),
                        hash: tree_id.to_string(),
                        source: Box::new(err),
                    })?
                    .ok_or_else(|| BackendError::ReadObject {
                        object_type: tree_id.object_type(),
                        hash: tree_id.to_string(),
                        source: Box::new(MultiBackendError(format!(
                            "Failed to read referenced tree {} as a part of a commit.",
                            tree_id.to_string()
                        ))),
                    })?;
                let root_path =
                    RepoPathBuf::from_internal_string(tree.root_path).map_err(|err| {
                        BackendError::ReadObject {
                            object_type: tree_id.object_type(),
                            hash: tree_id.to_string(),
                            source: Box::new(err),
                        }
                    })?;
                for sub_tree in tree.inner_trees {
                    let sub_tree_id = TreeId::from_bytes(&sub_tree.tree_id);
                    let total_path = root_path
                        .join(
                            RepoPathComponent::new(&sub_tree.path)
                                .expect("Couldn't parse tree path"),
                        )
                        .to_owned();
                    if !sub_tree.is_tree_multi_backend {
                        merge_conflict_per_path
                            .entry(total_path)
                            .or_insert(vec![])
                            .push((sub_tree_id, label.clone()))
                    } else {
                        let map_entry = next_trees_to_traverse_under.entry((sub_tree_id.clone(), total_path.as_internal_file_string().to_owned()));
                        if let Entry::Occupied(_occ) = map_entry {
                            return Err(BackendError::Other(Box::new(MultiBackendError(format!(
                                "Colliding tree ids while traversing! {} duplicated, duplicate is at {:?}",
                                sub_tree_id.to_string(),
                                total_path
                            )))));
                        }
                        map_entry.or_insert(label.clone());
                    }
                }
            }
            trees_to_traverse_under = next_trees_to_traverse_under;
        }
        return Ok(merge_conflict_per_path);
    }

    /// Make commits per inner backend.
    /// In particular, get the parents for each backend based on the parents
    /// here. Also get all the inner trees that are referenced by the trees
    /// provided here.
    fn split_commit_per_backend(
        &self,
        commit: &Commit,
    ) -> BackendResult<Vec<(RepoPathBuf, &Arc<dyn Backend>, Commit)>> {
        let mut parents_per_path = self.group_backends_by_commit_ids(commit.parents.iter())?;
        let mut predecessors_per_path =
            self.group_backends_by_commit_ids(commit.predecessors.iter())?;

        let mut merge_conflict_per_path = self.get_trees_inside_backends(
            &mut commit.root_tree.iter().zip(
                commit
                    .conflict_labels
                    .iter()
                    .chain(std::iter::repeat(&"".to_owned())),
            ),
        )?;
        merge_conflict_per_path
            .drain()
            .map(|(path, merges)| {
                let backend = self
                    .get_backend_at_path(path.as_ref())
                    .ok_or_else(|| {
                        BackendError::Other(Box::new(MultiBackendError(format!(
                            "Expected a backend at path {:?} but couldn't find one.",
                            path
                        ))))
                    })?
                    .0;

                let parents = parents_per_path.remove(&path).unwrap_or(vec![]);
                let predecessors = predecessors_per_path.remove(&path).unwrap_or(vec![]);

                Ok((
                    path,
                    backend,
                    Commit {
                        parents,
                        predecessors,
                        root_tree: Merge::from_vec(
                            merges
                                .iter()
                                .map(|(id, _label)| id.clone())
                                .collect::<Vec<TreeId>>(),
                        ),
                        conflict_labels: Merge::from_vec(
                            merges
                                .iter()
                                .map(|(_id, label)| label.to_string())
                                .collect::<Vec<String>>(),
                        ),
                        change_id: commit.change_id.clone(),
                        description: commit.description.clone(),
                        author: commit.author.clone(),
                        committer: commit.committer.clone(),
                        secure_sig: commit.secure_sig.clone(),
                    },
                ))
            })
            .collect::<BackendResult<Vec<(RepoPathBuf, &Arc<dyn Backend>, Commit)>>>()
    }
}

#[async_trait]
impl Backend for MultiBackend {
    fn name(&self) -> &str {
        Self::name()
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
                    source: Box::new(MultiBackendError(format!(
                        "Expected backend at path {:?} but couldn't find one.",
                        path
                    ))),
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
                    source: Box::new(MultiBackendError(format!(
                        "Expected backend at path {:?} but couldn't find one.",
                        path
                    ))),
                })?;
        backend.write_file(rest_of_path, contents).await
    }

    async fn read_symlink(&self, _path: &RepoPath, _id: &SymlinkId) -> BackendResult<String> {
        Err(BackendError::Unsupported(
            "Symlinking across backends is not supported.".to_string(),
        ))
    }

    async fn write_symlink(&self, _path: &RepoPath, _target: &str) -> BackendResult<SymlinkId> {
        Err(BackendError::Unsupported(
            "Symlinking across backends is not supported.".to_string(),
        ))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "The multi-backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn write_copy(&self, _copy: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "The multi-backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<CopyHistory>> {
        Err(BackendError::Unsupported(
            "The multi-backend doesn't support tracked copies yet".to_string(),
        ))
    }

    async fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        // println!("Read tree {:#?}: {:#?}", path, id);
        if !self.is_tree_at_path_multi_backend(path, id) {
            // TODO: don't traverse twice; just get the path and ID when checking where it's from.
            if let Some((backend, inner_path)) = self.get_backend_at_path(path) {
                return backend.read_tree(inner_path, id).await.map_err(|e| BackendError::ReadObject { object_type: id.object_type(), hash: id.to_string(), source: Box::new(e) });
            } else {
                panic!("Tree was thought to belong to a backend but apparently not.");
            }
        }

        let paths_and_trees = self.read_tree_proto(path, id)?;

        let mut entries: Vec<(RepoPathComponentBuf, TreeValue)> = paths_and_trees
            .inner_trees
            .iter()
            .map(|inner_tree| {
                (
                    RepoPathComponentBuf::new(inner_tree.path.clone())
                        .expect("Path couldn't be parsed."),
                    TreeValue::Tree(TreeId::from_bytes(&inner_tree.tree_id)),
                )
            })
            .collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        return Ok(Tree::from_sorted_entries(entries));
    }

    async fn write_tree(&self, path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        if let Some((backend, inner_path)) = self.get_backend_at_path(path) {
            return backend.write_tree(inner_path, contents).await;
        }

        self.write_multi_backend_tree(path, contents)
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
                source: Box::new(MultiBackendError(format!(
                    "Couldn't read commit {} for the data store",
                    id.to_string()
                ))),
            })?;

        Ok(Commit {
            parents: commit_proto
                .parent_ids
                .iter()
                .map(|i| CommitId::from_bytes(&i))
                .collect(),
            predecessors: commit_proto
                .predecessor_ids
                .iter()
                .map(|i| CommitId::from_bytes(&i))
                .collect(),
            root_tree: Merge::from_vec(
                commit_proto
                    .merge_data
                    .iter()
                    .map(|datum| TreeId::from_bytes(&datum.tree_id))
                    .collect::<Vec<TreeId>>(),
            ),
            conflict_labels: if commit_proto
                .merge_data
                .iter()
                .filter(|d| !d.conflict_label.is_empty())
                .next()
                .is_some()
            {
                Merge::from_vec(
                    commit_proto
                        .merge_data
                        .iter()
                        .map(|datum| datum.conflict_label.clone())
                        .collect::<Vec<String>>(),
                )
            } else {
                Merge::resolved("".to_string())
            },
            change_id: ChangeId::from_bytes(&commit_proto.change_id),
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
        mut sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let mut new_commit = MultiBackendCommit {
            id: blake2b_hash(&contents).to_vec(),
            parent_ids: contents.parents.iter().map(|c| c.to_bytes()).collect(),
            predecessor_ids: contents.predecessors.iter().map(|c| c.to_bytes()).collect(),
            inner_backend_commits: vec![],
            description: contents.description.clone(),
            author_name: contents.author.name.clone(),
            author_email: contents.author.email.clone(),
            author_timestamp: Some(crate::protos::multi_backend_commit::Timestamp {
                millis_since_epoch: contents.author.timestamp.timestamp.0,
                tz_offset: contents.author.timestamp.tz_offset,
            }),
            committer_name: contents.committer.name.clone(),
            committer_email: contents.committer.email.clone(),
            committer_timestamp: Some(crate::protos::multi_backend_commit::Timestamp {
                millis_since_epoch: contents.committer.timestamp.timestamp.0,
                tz_offset: contents.committer.timestamp.tz_offset,
            }),
            change_id: contents.change_id.to_bytes(),
            merge_data: contents
                .root_tree
                .iter()
                .zip(
                    contents
                        .conflict_labels
                        .iter()
                        // TODO: may be actually validate this?
                        .chain(std::iter::repeat(&"".to_owned())),
                )
                .map(
                    |(tree_id, label)| crate::protos::multi_backend_commit::LabeledMergeTrees {
                        tree_id: tree_id.to_bytes(),
                        conflict_label: label.to_owned(),
                    },
                )
                .collect(),
        };
        for (path, backend, commit) in self.split_commit_per_backend(&contents)? {
            let inner_commit_id = backend.write_commit(commit, sign_with.as_mut().map(|x| &mut **x)).await?.0;
            new_commit.inner_backend_commits.push(
                crate::protos::multi_backend_commit::InnerBackendCommit {
                    commit_id: inner_commit_id.to_bytes(),
                    path: path.as_internal_file_string().to_owned(),
                    deleted: false,
                },
            );
        }

        let output_commit_id = CommitId::from_bytes(&new_commit.id);
        let encoded_commit = new_commit.encode_to_vec();
        let (table, _lock) =
            self.commit_clock
                .get_head_locked()
                .map_err(|err| BackendError::WriteObject {
                    object_type: "commit",
                    source: Box::new(err),
                })?;
        let mut mut_table = table.start_mutation();
        mut_table.add_entry(new_commit.id, encoded_commit);
        self.commit_clock
            .save_table(mut_table)
            .map_err(|err| BackendError::WriteObject {
                object_type: "commit",
                source: Box::new(err),
            })?;

        return Ok((output_commit_id, contents));
    }

    fn get_copy_records(
        &self,
        paths: Option<&[RepoPathBuf]>,
        root: &CommitId,
        head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        let root_proto = self
            .read_table_at_commit(root)
            .map_err(|table_err| BackendError::ReadObject {
                object_type: root.object_type(),
                hash: root.to_string(),
                source: Box::new(table_err),
            })?
            .ok_or(BackendError::ReadObject {
                object_type: root.object_type(),
                hash: root.to_string(),
                source: Box::new(MultiBackendError(format!(
                    "Couldn't read commit {} for the data store",
                    root.to_string()
                ))),
            })?;
        let head_proto = self
            .read_table_at_commit(head)
            .map_err(|table_err| BackendError::ReadObject {
                object_type: head.object_type(),
                hash: head.to_string(),
                source: Box::new(table_err),
            })?
            .ok_or(BackendError::ReadObject {
                object_type: head.object_type(),
                hash: head.to_string(),
                source: Box::new(MultiBackendError(format!(
                    "Couldn't read commit {} for the data store",
                    head.to_string()
                ))),
            })?;

        let mut path_backend_commits_map: HashMap<RepoPathBuf, BackendAndCommits<'_>> = HashMap::new();
        for inner_commit in root_proto.inner_backend_commits {
            let path = RepoPath::from_internal_string(&inner_commit.path)
                .expect("Couldn't parse commit backend path");
            let backend = self.get_backend_at_path(path)
                    .ok_or_else(|| {
                        BackendError::Other(Box::new(MultiBackendError(format!(
                            "Expected a backend at path {:?} but couldn't find one.",
                            inner_commit.path
                        ))))})?.0;
            path_backend_commits_map.insert(
                path.to_owned(),
                BackendAndCommits::from_root_commit(backend, &inner_commit.commit_id, paths, path)
            );
        }

        for inner_commit in head_proto.inner_backend_commits {
            let path = RepoPath::from_internal_string(&inner_commit.path)
                .expect("Couldn't parse commit backend path");
            let map_entry = path_backend_commits_map.entry(path.to_owned());
            if let Entry::Occupied(mut backend_and_commits) = map_entry {
                backend_and_commits.get_mut().head = CommitId::from_bytes(inner_commit.commit_id.as_ref());
                continue;
            }
            let backend = self.get_backend_at_path(path)
                    .ok_or_else(|| {
                        BackendError::Other(Box::new(MultiBackendError(format!(
                            "Expected a backend at path {:?} but couldn't find one.",
                            inner_commit.path
                        ))))})?.0;
            map_entry.or_insert(
                BackendAndCommits::from_head_commit(backend, &inner_commit.commit_id, paths, path)
            );
        }

        let future_copies = path_backend_commits_map.drain().map(move |(path, backend_info): (RepoPathBuf, BackendAndCommits<'_>)| {
            backend_info.get_copy_records(path.to_owned(), head.clone(), root.clone())
        }).collect::<BackendResult<Vec<BoxStream<'_, BackendResult<CopyRecord>>>>>()?;
        Ok(Box::pin(futures::stream::select_all(future_copies)))

    }

    fn gc(&self, index: &dyn Index, keep_newer: SystemTime) -> BackendResult<()> {
        for maybe_backend in self.backends.values() {
            if let Some(backend) = maybe_backend {
                backend.gc(index, keep_newer)?;
            }
        }

        let (commit_table, _commit_lock) = self
            .commit_clock
            .get_head_locked()
            .map_err(|err| BackendError::Other(Box::new(err)))?;
        self.commit_clock
            .gc(&commit_table, keep_newer)
            .map_err(|err| BackendError::Other(Box::new(err)))?;
        let (tree_table, _tree_lock) = self
            .trees
            .get_head_locked()
            .map_err(|err| BackendError::Other(Box::new(err)))?;
        self.trees
            .gc(&tree_table, keep_newer)
            .map_err(|err| BackendError::Other(Box::new(err)))
    }
}

#[cfg(test)]
mod tests {

    use pollster::FutureExt as _;
    use tempfile::TempDir;

    use super::*;
    use crate::config::StackedConfig;
    use crate::git_backend::GitBackend;
    use crate::tests::new_temp_dir;

    const GIT_USER: &str = "Someone";
    const GIT_EMAIL: &str = "someone@example.com";

    fn create_signature() -> Signature {
        Signature {
            name: GIT_USER.to_string(),
            email: GIT_EMAIL.to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        }
    }

    fn user_settings() -> UserSettings {
        let config = StackedConfig::with_defaults();
        UserSettings::from_config(config).unwrap()
    }

    fn git_config() -> Vec<bstr::BString> {
        vec![
            format!("user.name = {GIT_USER}").into(),
            format!("user.email = {GIT_EMAIL}").into(),
            "init.defaultBranch = master".into(),
        ]
    }

    fn open_options() -> gix::open::Options {
        gix::open::Options::isolated()
            .config_overrides(git_config())
            .strict_config(true)
    }

    fn git_init(directory: impl AsRef<Path>) -> gix::Repository {
        gix::ThreadSafeRepository::init_opts(
            directory,
            gix::create::Kind::WithWorktree,
            gix::create::Options::default(),
            open_options(),
        )
        .unwrap()
        .to_thread_local()
    }

    fn make_git_backend() -> (Arc<dyn Backend>, gix::Repository, TempDir) {
        let settings = user_settings();
        let temp_dir = new_temp_dir();
        let store_path = temp_dir.path();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git_init(git_repo_path);
        let backend =
            Arc::new(GitBackend::init_external(&settings, store_path, git_repo.path()).unwrap());
        return (backend, git_repo, temp_dir);
    }

    #[test]
    fn read_written_tree_loop() {
        let (b1, git1, p1) = make_git_backend();
        let (b2, git2, p2) = make_git_backend();

        let repo1_path =
            RepoPathBuf::parse_fs_path(Path::new("/tmp"), Path::new("/tmp"), p1.path())
                .expect("Temp file path couldn't be parsed.");
        let repo2_path =
            RepoPathBuf::parse_fs_path(Path::new("/tmp"), Path::new("/tmp"), p2.path())
                .expect("Temp file path couldn't be parsed.");
        let (base_path, repo1_rel_path) = repo1_path.split_common_prefix(repo2_path.as_ref());
        let (base_path2, repo2_rel_path) = repo2_path.split_common_prefix(repo1_path.as_ref());
        assert_eq!(base_path, base_path2);

        let mut backend_tree = RepoPathTree::default();
        backend_tree.add(repo1_path.as_ref()).set_value(Some(b1));
        backend_tree.add(repo2_path.as_ref()).set_value(Some(b2));
        let multi_backend = MultiBackend::init_from_path(backend_tree, p1.path())
            .expect("Couldn't init the multi_backend from the path.");

        let create_tree = |i| {
            let blob_id1 = git1.write_blob(format!("content {i}")).unwrap();
            let mut tree_builder1 = git1.empty_tree().edit().unwrap();
            tree_builder1
                .upsert(
                    format!("file{i}"),
                    gix::object::tree::EntryKind::Blob,
                    blob_id1,
                )
                .unwrap();
            let id1 = TreeId::from_bytes(tree_builder1.write().unwrap().as_bytes());
            let blob_id2 = git2.write_blob(format!("content {i}")).unwrap();
            let mut tree_builder2 = git2.empty_tree().edit().unwrap();
            tree_builder2
                .upsert(
                    format!("file{i}"),
                    gix::object::tree::EntryKind::Blob,
                    blob_id2,
                )
                .unwrap();
            let id2 = TreeId::from_bytes(tree_builder2.write().unwrap().as_bytes());
            let mut entries: Vec<(RepoPathComponentBuf, TreeValue)> = vec![
                (
                    RepoPathComponentBuf::new(repo1_rel_path.as_internal_file_string())
                        .expect("Couldn't convert path to component."),
                    TreeValue::Tree(id1.clone()),
                ),
                (
                    RepoPathComponentBuf::new(repo2_rel_path.as_internal_file_string())
                        .expect("Couldn't convert path to component."),
                    TreeValue::Tree(id2.clone()),
                ),
            ];
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let multi_tree = multi_backend
                .write_tree(&base_path, &Tree::from_sorted_entries(entries))
                .block_on()
                .expect("Writing tree failed");
            (multi_tree, id1, id2)
        };

        let tree_ids = vec![
            create_tree(0),
            create_tree(1),
            create_tree(2),
            create_tree(3),
            create_tree(4),
        ];

        for (multi_id, id1, id2) in tree_ids.iter() {
            let multi_tree = multi_backend
                .read_tree(base_path, multi_id)
                .block_on()
                .expect("Failed to re-read multi-tree");
            let entries: Vec<crate::backend::TreeEntry<'_>> = multi_tree.entries().collect();
            assert_eq!(entries.len(), 2);
            for entry in entries.iter() {
                let inner_tree_id = match entry.value() {
                    TreeValue::Tree(id) => id,
                    _ => panic!("Inner tree was not a pointer to a tree"),
                };
                multi_backend
                    .read_tree(base_path.join(entry.name()).as_ref(), inner_tree_id)
                    .block_on()
                    .expect("Couldn't get inner tree.");
                if entry.name()
                    == RepoPathComponentBuf::new(repo1_rel_path.as_internal_file_string())
                        .expect("Couldn't convert path to component.")
                        .as_ref()
                {
                    assert_eq!(inner_tree_id, id1);
                } else if entry.name()
                    == RepoPathComponentBuf::new(repo2_rel_path.as_internal_file_string())
                        .expect("Couldn't convert path to component.")
                        .as_ref()
                {
                    assert_eq!(inner_tree_id, id2);
                } else {
                    panic!(
                        "Why does the upper tree have a path that isn't either of the inner repos?"
                    );
                }
            }
        }
    }

    #[test]
    fn read_written_commit_loop() {
        let (b1, git1, p1) = make_git_backend();
        let (b2, git2, p2) = make_git_backend();

        let repo1_path =
            RepoPathBuf::parse_fs_path(Path::new("/tmp"), Path::new("/tmp"), p1.path())
                .expect("Temp file path couldn't be parsed.");
        let repo2_path =
            RepoPathBuf::parse_fs_path(Path::new("/tmp"), Path::new("/tmp"), p2.path())
                .expect("Temp file path couldn't be parsed.");
        let (base_path, repo1_rel_path) = repo1_path.split_common_prefix(repo2_path.as_ref());
        let (base_path2, repo2_rel_path) = repo2_path.split_common_prefix(repo1_path.as_ref());
        assert_eq!(base_path, base_path2);

        let mut backend_tree = RepoPathTree::default();
        backend_tree.add(repo1_path.as_ref()).set_value(Some(b1));
        backend_tree.add(repo2_path.as_ref()).set_value(Some(b2));
        let multi_backend = MultiBackend::init_from_path(backend_tree, p1.path())
            .expect("Couldn't init the multi_backend from the path.");

        let create_tree = |i| {
            let blob_id1 = git1.write_blob(format!("content {i}")).unwrap();
            let mut tree_builder1 = git1.empty_tree().edit().unwrap();
            tree_builder1
                .upsert(
                    format!("file{i}"),
                    gix::object::tree::EntryKind::Blob,
                    blob_id1,
                )
                .unwrap();
            let id1 = TreeId::from_bytes(tree_builder1.write().unwrap().as_bytes());
            let blob_id2 = git2.write_blob(format!("content {i}")).unwrap();
            let mut tree_builder2 = git2.empty_tree().edit().unwrap();
            tree_builder2
                .upsert(
                    format!("file{i}"),
                    gix::object::tree::EntryKind::Blob,
                    blob_id2,
                )
                .unwrap();
            let id2 = TreeId::from_bytes(tree_builder2.write().unwrap().as_bytes());
            let mut entries: Vec<(RepoPathComponentBuf, TreeValue)> = vec![
                (
                    RepoPathComponentBuf::new(repo1_rel_path.as_internal_file_string())
                        .expect("Couldn't convert path to component."),
                    TreeValue::Tree(id1.clone()),
                ),
                (
                    RepoPathComponentBuf::new(repo2_rel_path.as_internal_file_string())
                        .expect("Couldn't convert path to component."),
                    TreeValue::Tree(id2.clone()),
                ),
            ];
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let multi_tree = multi_backend
                .write_tree(&base_path, &Tree::from_sorted_entries(entries))
                .block_on()
                .expect("Writing tree failed");
            return multi_tree;
        };

        let root_tree = Merge::from_removes_adds(
            vec![create_tree(0), create_tree(1)],
            vec![create_tree(2), create_tree(3), create_tree(4)],
        );

        let mut commit = Commit {
            parents: vec![multi_backend.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: root_tree.clone(),
            conflict_labels: Merge::resolved(String::new()),
            change_id: ChangeId::from_hex("abc123"),
            description: "".to_string(),
            author: create_signature(),
            committer: create_signature(),
            secure_sig: None,
        };

        let write_commit = |commit: Commit| -> BackendResult<(CommitId, Commit)> {
            multi_backend.write_commit(commit, None).block_on()
        };
        let (read_commit_id, commit) =
            write_commit(commit.clone()).expect("Failed to write commit.");
        let read_commit = multi_backend
            .read_commit(&read_commit_id)
            .block_on()
            .expect("Failed to read commit");
        assert_eq!(read_commit, commit);
    }
}
