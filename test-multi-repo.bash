#!/usr/bin/env bash

# https://stackoverflow.com/a/246128
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
JJ_BINARY=${JJ_BINARY:-${SCRIPT_DIR}/target/debug/jj}

${JJ_BINARY} version

cd /tmp
rm -rf jj-multi-repo-test
mkdir jj-multi-repo-test
cd jj-multi-repo-test

mkdir git1
cd git1
git init .
cd ..

mkdir git2
cd git2
git init .
cd ..

echo "test 1" >> git1/README.md
echo "test 2" >> git2/README.md


${JJ_BINARY} --debug multi-repo init $(find . -name .git)

echo "Inited repo"

${JJ_BINARY} --debug log

echo "Could log"

${JJ_BINARY} --debug --no-pager diff

echo "Diffed"
