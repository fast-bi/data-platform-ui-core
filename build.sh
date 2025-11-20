#!/bin/bash
#

set -o errexit
catch() {
    echo 'catching!'
    if [ "$1" != "0" ]; then
    # error handling goes here
    echo "Error $1 occurred on $2"
    fi
}
trap 'catch $? $LINENO' EXIT

dbt_init_version="v2.1.7"

docker buildx build . \
  --pull \
  --tag europe-central2-docker.pkg.dev/fast-bi-common/bi-platform/tsb-fastbi-web-core:${dbt_init_version} \
  --platform linux/amd64 \
  --push

docker buildx build . \
  --pull \
  --tag 4fastbi/data-platform-ui-core:dev-latest \
  --tag 4fastbi/data-platform-ui-core:dev-v0.1.3 \
  --platform linux/amd64 \
  --push