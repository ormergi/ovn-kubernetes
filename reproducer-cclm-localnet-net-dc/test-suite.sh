#! /bin/bash
##
## This script runs the project Kubevirt Live migration with localnet E2E tests, including CCLM test.
##
## The test create VM with secondary localnet network on the source cluster and migrate the VM
## to the target cluster, while measuring network outage on post migration, in other words
## how long does it take for VM network connectivity recover after migration.
##
## The test suite expect the following env vars in order to interact with the migration
## source and target clusters:
## - KUBECONFIG - Migration source cluster kubeconfig (example: $HOME/kind-ovn1.conf)
## - TARGET_CLUSTER_CONF - Migration target cluster kubeconfig (example: $HOME/kind-ovn2.conf)
## - TARGET_CLUSTER_API_URL - Migration target cluster API URL (example: https://127.0.0.1:55555).
##
## The script enable running the test suite multiple times.
## Controlled by $RUNS env var (default: 1)
##
## The script produce logs located right next to the script location, for example:
## └─ test-suite.sh
## └─ artifacts/
##    └─ 2025-01-01--12-00-00--migration-w-localnet/
##    └─ test-suite.log
##       ...
## - test-suite.log - This file execution transcript.

set -e

SCRIPT_PATH=$(dirname $(realpath -s $0))

TEST_DIR="$(realpath $SCRIPT_PATH/../test)"

OCI_BIN=${OCI_BIN:-podman}

CLUSTER_SOURCE="kind-ovn1"
CLUSTER_TARGET="kind-ovn2"
CLUSTER_SOURCE_KUBECONF="${HOME}/${CLUSTER_SOURCE}.conf"
CLUSTER_TARGET_KUBECONF="${HOME}/${CLUSTER_TARGET}.conf"

LOCAL_REGISTRY="localhost:5000"

VM_IMAGE="quay.io/kubevirtci/fedora-with-test-tooling:v20250416-e37573e"
IPERF_IMAGE="docker.io/nicolaka/netshoot:v0.14"
LOCAL_IPERF_IMAGE="localhost:5000/nicolaka/netshoot:v0.14"

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

underlay_iface_name=$($OCI_BIN network inspect underlay -f {{.NetworkInterface}})

mirror_image_to_local_registry $VM_IMAGE
mirror_image_to_local_registry $IPERF_IMAGE
cleanup_before_test_suite

dest_cluster_url=$(kubectl --kubeconfig=$CLUSTER_TARGET_KUBECONF cluster-info | head -1 | grep -Po "https://127.0.0.1:\d+")
[ -z $dest_cluster_url ] && echo "FATAL: could not get target cluster API URL" && exit 1

RUNS=${RUNS:-1}
runs=0
for r in $(seq $RUNS); do
  ts="$(date "+%Y-%m-%d--%H-%M-%S")"
  dir="${SCRIPT_PATH}/artifacts/${ts}--migration-w-localnet"
  mkdir -p $dir
  (
    echo "##### running test suite ($r/$RUNS) #####"
    export KUBECONFIG=$(realpath $CLUSTER_SOURCE_KUBECONF)
    export TARGET_CLUSTER_CONF=$(realpath $CLUSTER_TARGET_KUBECONF)
    export TARGET_CLUSTER_API_URL=$dest_cluster_url
    
    export TEST_REPORT_DIR="$dir"
    export FLAKE_ATTEMPTS=0
    export CONTAINER_RUNTIME=$OCI_BIN
    export HOST_UNDERLAY_IFACE=$underlay_iface_name
    export IPERF3_IMAGE=$LOCAL_IPERF_IMAGE
    export NETSHOOT_IMAGE=$LOCAL_IPERF_IMAGE
    export PLATFORM_IPV4_SUPPORT=true
    export PLATFORM_IPV6_SUPPORT=true
    export ENABLE_MULTI_NET=true
    export ENABLE_NETWORK_SEGMENTATION=true
    
    export WHAT="Kubevirt Virtual Machines live migration with localnet udn should maintain tcp connection with minimal downtime after succeeded"
    # LM between nodes scenario
    # export WHAT="Kubevirt Virtual Machines live migration with localnet udn should maintain tcp connection with minimal downtime after succeeded live migration"
    # cross-cluster LM scenario
    # export WHAT="Kubevirt Virtual Machines live migration with localnet udn should maintain tcp connection with minimal downtime after succeeded cross-cluster live migration"
    # LM with P-UDN L2  
    # export WHAT="Kubevirt Virtual Machines with user defined networks and persistent ips configured should keep ip after live migration of VirtualMachine with interface binding for UDN with Primary/Layer2 with snat ingress"
    make -C $TEST_DIR control-plane || true # skip error since we run test suite multiple times
    echo "#######################################"
  ) | tee "${dir}/test-suite.log"
  runs=$((++runs))
done

echo "DONE - ran $runs/$RUNS times"
