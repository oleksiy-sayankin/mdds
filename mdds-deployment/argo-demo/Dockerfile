# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

#
# SECURITY REVIEW (docker:S6471):
# This local-only demo intentionally runs the official K3s server image as
# root. K3s acts as both the control-plane and worker node and requires
# privileged access to manage containerd, networking, mounts, and cgroups.
#
# The image is pinned by digest, Argo Server is published only on 127.0.0.1,
# no Docker socket or host paths are mounted, and the environment is intended
# to execute only trusted manifests, images, and workflows.
#
# Rootless K3s is experimental and requires a different runtime architecture;
# simply setting a non-root Dockerfile USER is not a supported replacement.
#
FROM rancher/k3s@sha256:6a47cea22c4b834d4ba72c89d291696b79ebe406251f90b446e4dff03513dd87

#
# Bundle all Kubernetes bootstrap manifests in the image. At container
# startup, entrypoint.sh copies them to the K3s auto-deploy directory. K3s
# applies the ordinary Kubernetes resources and its Helm Controller installs
# Argo Workflows from the HelmChart resource.
#
COPY mdds-deployment/argo-demo/manifests/ /opt/argo-bootstrap/manifests/

#
# Bundle a sample Argo Workflow for a manual smoke test. This COPY instruction
# only adds the manifest to the image; it does not submit or execute it.
#
COPY mdds-deployment/argo-demo/smoke-tests/argo-hello-world.yaml /opt/argo-bootstrap/smoke-tests/argo-hello-world.yaml

# Bundle the startup script and make it executable. It places the HelmChart
# manifest in the K3s auto-deploy directory and then starts K3s.
COPY --chmod=0755 mdds-deployment/argo-demo/scripts/entrypoint.sh /opt/argo-bootstrap/bin/entrypoint.sh

# Bundle the executable health-check script used by Docker below.
COPY --chmod=0755 mdds-deployment/argo-demo/scripts/healthcheck.sh /opt/argo-bootstrap/bin/healthcheck.sh

# Document that Argo Server listens through NodePort 32746 inside the
# container. EXPOSE does not publish the port on the host; the container
# runtime or Docker Compose must map it separately.
EXPOSE 32746

# Periodically verify that the Kubernetes API, MinIO bootstrap, Argo Workflow
# CRD, Workflow Controller, and Argo Server are ready.
HEALTHCHECK --interval=10s --timeout=5s --start-period=5m --retries=30 CMD ["/opt/argo-bootstrap/bin/healthcheck.sh"]

# Ask Docker to send SIGTERM when stopping the container so K3s can shut down
# gracefully.
STOPSIGNAL SIGTERM

# Run the bootstrap script as the container entrypoint. The script ultimately
# replaces itself with /bin/k3s and passes it the CMD arguments below.
ENTRYPOINT ["/opt/argo-bootstrap/bin/entrypoint.sh"]

# Start a single K3s server that acts as both the control-plane and worker node.
# Single-server K3s uses its embedded SQLite datastore by default. Disable the
# packaged Traefik ingress controller and ServiceLB because this demo exposes
# Argo Server directly through NodePort.
CMD ["server", "--node-name=argo-k3s", "--disable=traefik", "--disable=servicelb"]
