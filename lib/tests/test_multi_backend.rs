use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use jj_lib::backend::Backend;
use jj_lib::backend::BackendLoadError;
use jj_lib::config::StackedConfig;
use jj_lib::multi_backend::MultiBackend;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo;
use jj_lib::repo::RepoLoader;
use jj_lib::repo::StoreFactories;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::repo_path::RepoPathTree;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use tempfile::TempDir;
use testutils::create_tree;
use testutils::repo_path_component;
use testutils::test_backend::TestBackend;
use testutils::test_backend::TestBackendData;
use testutils::test_backend::TestBackendFactory;

const REPO_WORKDIR_NAME: &str = "multi_backend_workdir";
const REPO_FILESDIR_NAME: &str = "multi_backend_files";

struct TestMultiBackendEnvironment {
    temp_dir: TempDir,
    inner_repos: HashMap<RepoPathBuf, Arc<TestBackend>>,
}

impl TestMultiBackendEnvironment {
    fn create_flat_repos() -> Self {
        let temp_dir = TempDir::new().expect("Couldn't make tmp dir");
        std::fs::create_dir(temp_dir.path().join(REPO_FILESDIR_NAME))
            .expect("Failed to make the files dir");
        let inner_repos = HashMap::from([
            (
                RepoPathBuf::parse_fs_path(
                    Path::new("/tmp"),
                    Path::new("/tmp"),
                    temp_dir.path().join(REPO_FILESDIR_NAME).join("repo1"),
                )
                .expect("Temp file path couldn't be parsed."),
                Arc::new(TestBackend::with_data(Arc::new(Mutex::new(
                    TestBackendData::default(),
                )))),
            ),
            (
                RepoPathBuf::parse_fs_path(
                    Path::new("/tmp"),
                    Path::new("/tmp"),
                    temp_dir.path().join(REPO_FILESDIR_NAME).join("repo2"),
                )
                .expect("Temp file path couldn't be parsed."),
                Arc::new(TestBackend::with_data(Arc::new(Mutex::new(
                    TestBackendData::default(),
                )))),
            ),
        ]);
        Self {
            temp_dir,
            inner_repos,
        }
    }

    fn create_nested_repos() -> Self {
        let temp_dir = TempDir::new().expect("Couldn't make tmp dir");
        std::fs::create_dir(temp_dir.path().join(REPO_FILESDIR_NAME))
            .expect("Failed to make the files dir");
        let inner_repos = HashMap::from([
            (
                RepoPathBuf::parse_fs_path(
                    Path::new("/tmp"),
                    Path::new("/tmp"),
                    temp_dir.path().join(REPO_FILESDIR_NAME).join("repo1"),
                )
                .expect("Temp file path couldn't be parsed."),
                Arc::new(TestBackend::with_data(Arc::new(Mutex::new(
                    TestBackendData::default(),
                )))),
            ),
            (
                RepoPathBuf::parse_fs_path(
                    Path::new("/tmp"),
                    Path::new("/tmp"),
                    temp_dir
                        .path()
                        .join(REPO_FILESDIR_NAME)
                        .join("repo1")
                        .join("repo2"),
                )
                .expect("Temp file path couldn't be parsed."),
                Arc::new(TestBackend::with_data(Arc::new(Mutex::new(
                    TestBackendData::default(),
                )))),
            ),
        ]);
        Self {
            temp_dir,
            inner_repos,
        }
    }

    fn into_multi_repo_and_dir(self) -> (Arc<ReadonlyRepo>, HashSet<RepoPathBuf>, TempDir) {
        std::fs::create_dir(self.temp_dir.path().join(REPO_WORKDIR_NAME))
            .expect("Failed to make the work dir");
        let config = StackedConfig::with_defaults();
        let settings = UserSettings::from_config(config).unwrap();
        let repo_paths: HashSet<RepoPathBuf> =
            self.inner_repos.keys().map(|p| p.to_owned()).collect();
        let repo = ReadonlyRepo::init(
            &settings,
            self.temp_dir.path().join(REPO_FILESDIR_NAME).as_ref(),
            &|settings, path| {
                MultiBackend::init_with_backends_at_paths(
                    settings,
                    &repo_paths,
                    path,
                    &|_settings, repo_path, _store_path| {
                        let path_buf = RepoPathBuf::from_relative_path(repo_path)
                            .expect("Couldn't parse path");
                        Ok(self.inner_repos.get(&path_buf).unwrap().clone())
                    },
                )
                .map(|b: MultiBackend| -> Box<dyn Backend> { Box::new(b) })
            },
            Signer::from_settings(&settings).unwrap(),
            ReadonlyRepo::default_op_store_initializer(),
            ReadonlyRepo::default_op_heads_store_initializer(),
            ReadonlyRepo::default_index_store_initializer(),
            ReadonlyRepo::default_submodule_store_initializer(),
        )
        .expect("Failed to init repo");
        (repo, repo_paths, self.temp_dir)
    }
}

// TODO: is this a useful test?
#[test]
fn test_create_repo() {
    TestMultiBackendEnvironment::create_flat_repos().into_multi_repo_and_dir();
}

#[test]
fn test_write_commit() {
    let (repo, repo_paths, _dir) =
        TestMultiBackendEnvironment::create_flat_repos().into_multi_repo_and_dir();
    let test_contents: Vec<(RepoPathBuf, String)> = repo_paths
        .iter()
        .enumerate()
        .map(|(i, p)| {
            (
                p.join(repo_path_component("file")),
                format!("Text in file {}", i),
            )
        })
        .collect();
    let path_contents_vec: Vec<(&RepoPath, &str)> = test_contents
        .iter()
        .map(|(p, c)| (p.as_ref(), c.as_ref()))
        .collect();
    let tree = create_tree(&repo, &path_contents_vec);
    let mut tx = repo.start_transaction();
    let mut_repo = tx.repo_mut();
    let commit = mut_repo
        .new_commit(vec![mut_repo.store().root_commit_id().clone()], tree)
        .set_description("test: put text in files for testing.")
        .write()
        .expect("Couldn't write commit.");
    mut_repo
        .set_wc_commit(WorkspaceName::DEFAULT.to_owned(), commit.id().clone())
        .expect("Setting working copy commit failed.");
}

#[test]
fn test_write_commit_in_nested() {
    let (repo, repo_paths, _dir) =
        TestMultiBackendEnvironment::create_nested_repos().into_multi_repo_and_dir();
    let test_contents: Vec<(RepoPathBuf, String)> = repo_paths
        .iter()
        .enumerate()
        .map(|(i, p)| {
            (
                p.join(repo_path_component("file")),
                format!("Text in file {}", i),
            )
        })
        .collect();
    let path_contents_vec: Vec<(&RepoPath, &str)> = test_contents
        .iter()
        .map(|(p, c)| (p.as_ref(), c.as_ref()))
        .collect();
    let tree = create_tree(&repo, &path_contents_vec);
    let mut tx = repo.start_transaction();
    let mut_repo = tx.repo_mut();
    let commit = mut_repo
        .new_commit(vec![mut_repo.store().root_commit_id().clone()], tree)
        .set_description("test: put text in files for testing.")
        .write()
        .expect("Couldn't write commit.");
    mut_repo
        .set_wc_commit(WorkspaceName::DEFAULT.to_owned(), commit.id().clone())
        .expect("Setting working copy commit failed.");
}

#[test]
fn test_init_act_reload() {
    // Init and act.
    let (commit, dir) = {
        let (repo, repo_paths, dir) =
            TestMultiBackendEnvironment::create_flat_repos().into_multi_repo_and_dir();
        let test_contents: Vec<(RepoPathBuf, String)> = repo_paths
            .iter()
            .enumerate()
            .map(|(i, p)| {
                (
                    p.join(repo_path_component("file")),
                    format!("Text in file {}", i),
                )
            })
            .collect();
        let path_contents_vec: Vec<(&RepoPath, &str)> = test_contents
            .iter()
            .map(|(p, c)| (p.as_ref(), c.as_ref()))
            .collect();
        let tree = create_tree(&repo, &path_contents_vec);
        let mut tx = repo.start_transaction();
        let mut_repo = tx.repo_mut();
        (
            mut_repo
                .new_commit(vec![mut_repo.store().root_commit_id().clone()], tree)
                .set_description("test: put text in files for testing.")
                .write()
                .expect("Couldn't write commit."),
            dir,
        )
    };

    // Reload
    let mut factories = StoreFactories::default();
    factories.add_backend("test", {
        Box::new(move |_settings, _store_path, _store_factories| {
            Ok(Box::new(TestBackend::with_data(Arc::new(Mutex::new(
                TestBackendData::default(),
            )))))
        })
    });
    let repo = {
        let config = StackedConfig::with_defaults();
        let settings = UserSettings::from_config(config).unwrap();
        RepoLoader::init_from_file_system(
            &settings,
            dir.path().join(REPO_FILESDIR_NAME).as_ref(),
            &factories,
        )
        .expect("Failed to init repo loader")
        .load_at_head()
        .expect("Could not load repo at head")
    };
    let mut tx = repo.start_transaction();
    let mut_repo = tx.repo_mut();
    mut_repo
        .set_wc_commit(WorkspaceName::DEFAULT.to_owned(), commit.id().clone())
        .expect("Setting working copy commit failed.");
}
