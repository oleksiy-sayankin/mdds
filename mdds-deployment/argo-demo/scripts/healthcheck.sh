#!/bin/sh

# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

set -eu

kube() {
  /bin/kubectl "$@"
}

kube get --raw=/readyz >/dev/null 2>&1
kube --namespace mdds-storage rollout status \
  deployment/minio \
  --timeout=2s >/dev/null 2>&1
kube --namespace mdds-storage wait \
  --for=condition=complete \
  job/minio-bootstrap \
  --timeout=2s >/dev/null 2>&1
kube get crd workflows.argoproj.io >/dev/null 2>&1
kube --namespace argo rollout status \
  deployment/argo-workflows-workflow-controller \
  --timeout=2s >/dev/null 2>&1
kube --namespace argo rollout status \
  deployment/argo-workflows-server \
  --timeout=2s >/dev/null 2>&1
