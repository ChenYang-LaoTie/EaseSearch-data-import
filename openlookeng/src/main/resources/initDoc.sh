#!/bin/bash
mkdir -p ${TARGET}/zh/
mkdir -p ${TARGET}/en/

if [ ! -d "${SOURCE}/website" ]; then
 rm -rf ${TARGET}
 exit
fi

# shellcheck disable=SC2164
cd ${SOURCE}/website

cp -r ${SOURCE}/website/app/.vitepress/dist/zh ${TARGET}/
cp -r ${SOURCE}/website/app/.vitepress/dist/en ${TARGET}/

rm -rf ${TARGET}/zh/blogs
cp -r ${SOURCE}/website/app/zh/blogs ${TARGET}/zh/
rm -rf ${TARGET}/zh/news
cp -r ${SOURCE}/website/app/zh/news ${TARGET}/zh/
rm -rf ${TARGET}/zh/events
cp -r ${SOURCE}/website/app/zh/events ${TARGET}/zh/
rm -rf ${TARGET}/zh/userPractice
cp -r ${SOURCE}/website/app/zh/userPractice ${TARGET}/zh/

rm -rf ${TARGET}/en/blogs
cp -r ${SOURCE}/website/app/en/blogs ${TARGET}/en/
rm -rf ${TARGET}/en/news
cp -r ${SOURCE}/website/app/en/news ${TARGET}/en/
rm -rf ${TARGET}/en/events
cp -r ${SOURCE}/website/app/en/events ${TARGET}/en/
rm -rf ${TARGET}/en/userPractice
cp -r ${SOURCE}/website/app/en/userPractice ${TARGET}/en/

# shellcheck disable=SC2164
cd ${SOURCE}

if [ ! -d "${SOURCE}/docs" ]; then
 rm -rf ${TARGET}
 exit
fi

# shellcheck disable=SC2164
cd ./docs

for r in $(git branch -r --list "origin/*"); do
  b=${r##*origin/}
 # shellcheck disable=SC2170
 # shellcheck disable=SC1073
 # shellcheck disable=SC1072
 # shellcheck disable=SC1020
 # shellcheck disable=SC1009
 # shellcheck disable=SC2053
 if [[ "website" != $b ]] && [[ "HEAD" != $b ]] && [[ "->" != $b ]] && [[ "reconstruct-frozen" != $b ]] && [[ "master-bak" != $b ]] && [[ "website-v2" != $b ]] && [[ "z11" != $b ]]; then
    git checkout $r
    mkdir -p ${TARGET}/zh/docs/$b/docs
    mkdir -p ${TARGET}/en/docs/$b/docs
    cp -r ${SOURCE}/docs/content/zh/docs/* ${TARGET}/zh/docs/$b/docs/
    cp -r ${SOURCE}/docs/content/en/docs/* ${TARGET}/en/docs/$b/docs/

    mkdir -p ${TARGET}/zh/docs/$b-lite/docs
    mkdir -p ${TARGET}/en/docs/$b-lite/docs
    cp -r ${SOURCE}/docs/content/docs-lite/zh/docs/* ${TARGET}/zh/docs/$b-lite/docs/
    cp -r ${SOURCE}/docs/content/docs-lite/en/docs/* ${TARGET}/en/docs/$b-lite/docs/

 fi
done



