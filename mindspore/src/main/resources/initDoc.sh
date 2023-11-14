#!/bin/bash
SOURCE=/docs-file/source
TARGET=/docs-file/target
mkdir -p ${SOURCE}
mkdir -p ${TARGET}

# shellcheck disable=SC2164
cd ${SOURCE}

git clone https://gitee.com/mindspore/website-docs.git

if [ ! -d "${SOURCE}/website-docs" ]; then
 rm -rf ${TARGET}
 exit
fi


# shellcheck disable=SC2164
cd ${SOURCE}/website-docs

cp -r ${SOURCE}/website-docs/public/* ${TARGET}/

# shellcheck disable=SC2164
cd ${TARGET}/

# shellcheck disable=SC2035

rm -rf admin
rm -rf allow_sensor
rm -rf api
rm -rf apicc
rm -rf cla
rm -rf commonJs
rm -rf doc
rm -rf images
rm -rf lib
rm -rf more
rm -rf pdfjs
rm -rf pic
rm -rf security
rm -rf statement
rm -rf statics
rm -rf tutorial
rm -rf video
rm -rf mindscience
rm -rf vision

rm -f *

find . -type f -name file_include_\* -exec rm {} \;
find . -type f -name program_listing_file_include_\* -exec rm {} \;

# shellcheck disable=SC2038
find ./ -name _images |xargs rm -rf
# shellcheck disable=SC2038
find ./ -name _modules |xargs rm -rf
# shellcheck disable=SC2038
find ./ -name _sources |xargs rm -rf
# shellcheck disable=SC2038
find ./ -name _static |xargs rm -rf
# shellcheck disable=SC2038
find ./ -name search.html |xargs rm -rf
# shellcheck disable=SC2038
find ./ -name genindex.html |xargs rm -rf
# shellcheck disable=SC2038
find ./ -name py-modindex.html |xargs rm -rf
# shellcheck disable=SC2038
find ./ -name unabridged_api.html |xargs rm -rf