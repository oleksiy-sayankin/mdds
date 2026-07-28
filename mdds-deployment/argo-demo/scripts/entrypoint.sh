#!/bin/sh

# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

set -eu

# Paths used to copy every bundled bootstrap manifest into the K3s auto-deploy
# directory through a temporary file.
manifest_source_dir=/opt/argo-bootstrap/manifests
manifest_target_dir=/var/lib/rancher/k3s/server/manifests

# /var/lib/rancher/k3s is a Docker volume, so files copied there while the
# image is built would be hidden at runtime. Place every bootstrap manifest
# into the mounted volume immediately before K3s starts. Copy each manifest
# to a temporary file first, then atomically rename it so that the target
# becomes visible only after the manifest has been written completely.
mkdir -p "${manifest_target_dir}"

for manifest_source in "${manifest_source_dir}"/*.yaml; do
  manifest_name=${manifest_source##*/}
  manifest_target=${manifest_target_dir}/${manifest_name}
  manifest_temporary=${manifest_target}.tmp

  cp "${manifest_source}" "${manifest_temporary}"
  mv "${manifest_temporary}" "${manifest_target}"
done

exec /bin/k3s "$@"
