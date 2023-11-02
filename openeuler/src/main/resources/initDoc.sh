#!/bin/bash
mkdir -p ${TARGET}/zh/
mkdir -p ${TARGET}/en/
mkdir -p ${TARGET}/ru/

if [ ! -d ${SOURCE}/openEuler-portal ]; then
 rm -rf ${TARGET}
 exit
fi

# shellcheck disable=SC2164
cd ${SOURCE}/openEuler-portal

cp -r ${SOURCE}/openEuler-portal/app/.vitepress/dist/zh ${TARGET}/
cp -r ${SOURCE}/openEuler-portal/app/.vitepress/dist/en ${TARGET}/
cp -r ${SOURCE}/openEuler-portal/app/.vitepress/dist/ru ${TARGET}/


rm -rf ${TARGET}/zh/blog
cp -r ${SOURCE}/openEuler-portal/app/zh/blog ${TARGET}/zh/
rm -rf ${TARGET}/zh/news
cp -r ${SOURCE}/openEuler-portal/app/zh/news ${TARGET}/zh/
rm -rf ${TARGET}/zh/showcase
cp -r ${SOURCE}/openEuler-portal/app/zh/showcase ${TARGET}/zh/
cp ${SOURCE}/openEuler-portal/app/.vitepress/dist/zh/showcase/index.html ${TARGET}/zh/showcase/
rm -rf ${TARGET}/zh/migration
cp -r ${SOURCE}/openEuler-portal/app/zh/migration ${TARGET}/zh/

rm -rf ${TARGET}/en/blog
cp -r ${SOURCE}/openEuler-portal/app/en/blog ${TARGET}/en/
rm -rf ${TARGET}/en/news
cp -r ${SOURCE}/openEuler-portal/app/en/news ${TARGET}/en/
rm -rf ${TARGET}/en/showcase
cp -r ${SOURCE}/openEuler-portal/app/en/showcase ${TARGET}/en/
cp ${SOURCE}/openEuler-portal/app/.vitepress/dist/en/showcase/index.html ${TARGET}/en/showcase/
rm -rf ${TARGET}/en/migration
cp -r ${SOURCE}/openEuler-portal/app/en/migration ${TARGET}/en/

rm -rf ${TARGET}/ru/blog
cp -r ${SOURCE}/openEuler-portal/app/ru/blog ${TARGET}/ru/
rm -rf ${TARGET}/ru/news
cp -r ${SOURCE}/openEuler-portal/app/ru/news ${TARGET}/ru/
rm -rf ${TARGET}/ru/showcase
cp -r ${SOURCE}/openEuler-portal/app/ru/showcase ${TARGET}/ru/
cp ${SOURCE}/openEuler-portal/app/.vitepress/dist/ru/showcase/index.html ${TARGET}/ru/showcase/
rm -rf ${TARGET}/ru/migration
cp -r ${SOURCE}/openEuler-portal/app/ru/migration ${TARGET}/ru/


# shellcheck disable=SC2164
cd ${SOURCE}

if [ ! -d "${SOURCE}/docs" ]; then
 rm -rf ${TARGET}
 exit
fi

# shellcheck disable=SC2164
cd ./docs
for r in $(git branch -r --list "origin/stable2-*")
do
  b=${r##*origin/stable2-}
  git checkout $r
  mkdir -p ${TARGET}/zh/docs/$b/docs
  mkdir -p ${TARGET}/en/docs/$b/docs
  cp -r ${SOURCE}/docs/docs/zh/docs/* ${TARGET}/zh/docs/$b/docs/
  cp -r ${SOURCE}/docs/docs/en/docs/* ${TARGET}/en/docs/$b/docs/
done












