#! /bin/bash
## spike: network outage on post cross-cluster live migration with localnet UDNs
##
## This script runs the project e2e test suit for Kubevirt cross-cluster live migration (cclm).
##
## The test create VM with secondary localnet network on the source cluster and migrate the VM
## to the target cluster, while measuring network outage on post migration, in other words
## how long does it take for VM network connectivity recover after migration.
##
## The test suite expect the following env vars in order to interact with the migration
## target cluster:
## - KUBECONFIG - Migration Source cluster kubeconfig path (example: $HOME/ovn1.conf)
## - TARGET_CLUSTER_CONF - Migration target cluster kubeconfig path (example: $HOME/ovn2.conf)
## - TARGET_CLUSTER_API_URL - Migration target cluster API URL (example: https://127.0.0.1:55555).
##
## The script enable running the test suite multiple times, it is controlled by $TRIALS env var (default: 1)
##
## The script produce logs located right next to the script location, for example:
## └─ run-suite.sh
## └─ test-suite.log
## └─ artifacts/
##    └─ 2025-12-26--02-50-39--cluster-local-live-migration-w-localnet/
##       ├── test_172.31.0.10_iperf3.log
##       ├── test_2010:100:200::10_iperf3.log
##       └── test-stats
## - test-suite.log - This file execution transcript.
## - test.*iperf.log - Iperf3 log of the measurment from before, during and after the migration.
## - test-stat - Summerize how long did it take for VM network to recover on post migration.

set -e

SCRIPT_PATH=$(dirname $(realpath -s $0))

CLUSTER_SOURCE="kind-ovn1"
CLUSTER_TARGET="kind-ovn2"
CLUSTER_SOURCE_KUBECONF="${HOME}/${CLUSTER_SOURCE}.conf"
CLUSTER_TARGET_KUBECONF="${HOME}/${CLUSTER_TARGET}.conf"

LOCAL_REGISTRY="localhost:5000"

VM_IMAGE="quay.io/kubevirtci/fedora-with-test-tooling:v20250416-e37573e"
IPERF_IMAGE="docker.io/nicolaka/netshoot:v0.14"
LOCAL_IPERF_IMAGE="localhost:5000/nicolaka/netshoot:v0.14"
HTTPBIN_IMAGE="docker.io/kennethreitz/httpbin:latest"

mirror_image_to_local_registry() {
  local -r image_tag="$1"
  local -r repo_tag="${image_tag#*/}"
  local_image_tag="${LOCAL_REGISTRY}/${repo_tag}"
  if ! skopeo inspect "docker://${local_image_tag}" --tls-verify=false &> /dev/null; then
    echo "Mirror image ($image_tag) to local registry.."
    skopeo copy "docker://${image_tag}" "docker://${local_image_tag}" --dest-tls-verify=false
  fi
}

cleanup_before_test_suite(){
  set +e -x
  echo "Cleaning up dangling objects before test suite"
  for k in $CLUSTER_TARGET_KUBECONF $CLUSTER_SOURCE_KUBECONF; do
    ns=$(kubectl --kubeconfig=$k get ns | grep -Po "kv-test-migration-localnet-.{5}")
    if [[ -z $ns ]]; then continue; fi
    for v in $(kubectl --kubeconfig=$k -n $ns get vmim --no-headers -o custom-columns=:metadata.name); do
      kubectl --kubeconfig=$k -n $ns delete vmim $v --wait=false
      kubectl --kubeconfig=$k -n $ns patch vmim $v --type=merge -p '{"metadata":{"finalizers":null}}' ||:
    done
    kubectl --kubeconfig=$k -n $ns delete vm --all --wait=false
    kubectl --kubeconfig=$k -n $ns delete pod --all --wait=false
    kubectl --kubeconfig=$k get clusteruserdefinednetwork | grep kv-test | awk '{print $1}' | xargs kubectl --kubeconfig=$k delete clusteruserdefinednetwork --wait=false
  done
  for k in $CLUSTER_TARGET_KUBECONF $CLUSTER_SOURCE_KUBECONF; do
    ns=$(kubectl --kubeconfig=$k get ns | grep -Po "kv-test-migration-localnet-.{5}")
    if [[ -z $ns ]]; then continue; fi
    kubectl --kubeconfig=$k delete ns $ns
  done
  set -e +x
}

mirror_image_to_local_registry $VM_IMAGE
mirror_image_to_local_registry $IPERF_IMAGE
mirror_image_to_local_registry $HTTPBIN_IMAGE
cleanup_before_test_suite

dest_cluster_url=$(kubectl --kubeconfig=$CLUSTER_TARGET_KUBECONF cluster-info | head -1 | grep -Po "https://127.0.0.1:\d+")
[ -z $dest_cluster_url ] && echo "FATAL: could not get target cluster API URL" && exit 1

TRIALS=${TRIALS:-1}
runs=0
for t in $(seq $TRIALS); do
  ts="$(date "+%Y-%m-%d--%H-%M-%S")"
  dir="${SCRIPT_PATH}/artifacts/${ts}--migration-w-localnet"
  mkdir -p $dir
  start_time_seconds=$(date +%s)
  (
    echo "##### running test suite (trial $t/$TRIALS) #####"
    export KUBECONFIG=$(realpath $CLUSTER_SOURCE_KUBECONF)
    export TARGET_CLUSTER_CONF=$(realpath $CLUSTER_TARGET_KUBECONF)
    export TARGET_CLUSTER_API_URL=$dest_cluster_url
    
    export TEST_REPORT_DIR="$dir"
    export FLAKE_ATTEMPTS=0
    export CONTAINER_RUNTIME=podman
    export IPERF3_IMAGE=$LOCAL_IPERF_IMAGE
    export PLATFORM_IPV4_SUPPORT=true
    export PLATFORM_IPV6_SUPPORT=true
    export ENABLE_MULTI_NET=true
    export ENABLE_NETWORK_SEGMENTATION=true
    
    export HOST_UNDERLAY_IFACE="podman2"
    
    export WHAT="Kubevirt Virtual Machines live migration with localnet udn should maintain tcp connection with minimal downtime after succeeded"
    # LM between nodes scenerio
    # export WHAT="Kubevirt Virtual Machines live migration with localnet udn should maintain tcp connection with minimal downtime after succeeded live migration"
    # cross-cluster LM scenerio
    # export WHAT="Kubevirt Virtual Machines live migration with localnet udn should maintain tcp connection with minimal downtime after succeeded cross-cluster live migration"
    # LM with P-UDN L2  
    # export WHAT="Kubevirt Virtual Machines with user defined networks and persistent ips configured should keep ip after live migration of VirtualMachine with interface binding for UDN with Primary/Layer2 with snat ingress"
    make -C test control-plane || true # skip error since we run test suite multiple times
    echo "#######################################"
  ) | tee "${dir}/test-suite.log"
  
  run_duration_seconds=$(($(date +%s) - $start_time_seconds))
  runs=$((++runs))
done

echo "DONE - ran $runs/$TRIALS times"
