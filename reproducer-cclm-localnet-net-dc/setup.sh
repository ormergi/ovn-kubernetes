#! /bin/bash 

help() {
  echo "Usage: $(basename $0) [OPTIONS]"
  echo ""
  echo "Creates development environment for testing Kubevirt cross-cluster Live-Migration (CCLM)"
  echo "with OVN-Kubernetes Localnet user-defined networks."
  echo """"
  echo "The environment consist of two KinD clusters, source and target clusters,"
  echo "where the source cluster is where the VM initially created and then migrated"
  echo "to the target cluster."
  echo "Each cluster has OVN-Kubernetes, Kubevirt installed."
  echo """"
  echo "The clusters are connected with each other over the following networks:"
  echo "- kind: Clusters nodes network"
  echo "- migration: Dedicated network for Kubevirt live-migration"
  echo "- underlay: Secondary network, VM will be connected to via OVN-Kubernetes Localnet user-defined network (UDN)"
  echo ""
  echo "OPTIONS:"
  echo "--cluster-up             Spin up two KinD clusters with OVN-Kubernetes and Kubevirt"
  echo "--cluster-down           Teardown the environment"
  echo "--cluster-sync           Sync ovn-kubernetes instances"
  echo "--cluster-sync-target    Sync source cluster ovn-kubernetes instance on"
  echo "--cluster-sync-source    Sync target cluster ovn-kubernetes instance on"
  echo "--register-kubevirt      Register source and target cluster kubevirt instances to enable CCLM"
  echo "--help                   Display help and exit"
  echo ""
}

set -e

SCRIPT_PATH=$(dirname $(realpath -s $0))

MANIFESTS="${SCRIPT_PATH}/manifests"

CONTRIB="$(realpath ${SCRIPT_PATH}/../contrib)"

CLUSTER_SOURCE="kind-ovn1"
CLUSTER_TARGET="kind-ovn2"
CLUSTER_SOURCE_CFG="${HOME}/${CLUSTER_SOURCE}.conf"
CLUSTER_TARGET_CFG="${HOME}/${CLUSTER_TARGET}.conf"

BR_CNI="${MANIFESTS}/bridgecni"
WHEREABOUTS="${MANIFESTS}/whereaboutscni"
NAD_MIGRATION_NET_SOURCE="${MANIFESTS}/nad-source-migration-network"
NAD_MIGRATION_NET_TARGET="${MANIFESTS}/nad-target-migration-network"
KUBEVIRT_PATCH="${MANIFESTS}/kv-patch"

BR_CNI_IMAGE="quay.io/kubevirt/cni-default-plugins:v1.5.1"
WHEREABOUTS_IMAGE="ghcr.io/k8snetworkplumbingwg/whereabouts:latest"

BR_MAPPING="localnet1"
BR_NAME="ovsbr1"
UNDERLAY_NIC="eth1"

MIGRATION_NETWORK="migration"
MIGRATION_BR="br-mig"
MIGRATION_IFACE="eth2"

export OCI_BIN=${OCI_BIN:-podman}
export KUBEVIRT_VERSION=${KUBEVIRT_VERSION:-"v1.7.0"}

ensure_prerequisites() {
  local -r rpms=(pip3 openssl wget virtctl)
  for r in "${rpms[@]}"; do
    if ! which $r &> /dev/null; then
       dnf install $r
    fi
  done

  local -r pip_libs=(jinjanator)
  for l in "${pip_libs[@]}"; do
    if ! pip3 show $l &> /dev/null; then
       pip3 install $l
    fi
  done

  if ! which virtctl &> /dev/null; then
    echo "install virtctl"
    v=$(curl https://storage.googleapis.com/kubevirt-prow/release/kubevirt/kubevirt/stable.txt)
    wget https://github.com/kubevirt/kubevirt/releases/download/${v}/virtctl-${v}-linux-amd64
    chmod +x "virtctl-$v-linux-amd64"
    cp virtctl-$v-linux-amd64 /usr/bin/virtctl
  fi
}

configure_kernel_params() {
    if [[ $(sysctl fs.inotify.max_user_instances -n) -lt 1048576 ]]; then
        sudo sysctl -w fs.inotify.max_user_instances=1048576
    fi
    if [[ $(sysctl fs.inotify.max_user_instances -n) -lt 1024 ]]; then
        sudo sysctl -w fs.inotify.max_user_instances=1024
    fi
    if [[ $(sysctl net.ipv6.conf.all.forwarding -n) -ne 1 ]]; then
        sudo sysctl -w net.ipv6.conf.all.forwarding=1
    fi
}

build_ovn_image() {
    export SKIP_INSTALL_OVN=true SKIP_DEPLOY_COMPONENTS=true
    ovn_cluster_up $CLUSTER_SOURCE $CLUSTER_SOURCE_CFG --deploy
    unset SKIP_INSTALL_OVN SKIP_DEPLOY_COMPONENTS
}

cluster_up() {
    export SKIP_OVN_BUILD=true
    declare -A JOBS
    if [[ ! $(kind get clusters -q) =~ $CLUSTER_SOURCE ]]; then
      echo "Creating source cluster ($CLUSTER_SOURCE).."
      ovn_cluster_up $CLUSTER_SOURCE $CLUSTER_SOURCE_CFG &> "${SCRIPT_PATH}/${CLUSTER_SOURCE}.log" &
      JOBS["$!"]="$CLUSTER_SOURCE"
    fi
    if [[ ! $(kind get clusters -q) =~ $CLUSTER_TARGET ]]; then
      sleep 10
      echo "Creating target cluster ($CLUSTER_TARGET).."
      ovn_cluster_up $CLUSTER_TARGET $CLUSTER_TARGET_CFG &> "${SCRIPT_PATH}/${CLUSTER_TARGET}.log" &
      JOBS["$!"]="$CLUSTER_TARGET"
    fi
    unset SKIP_OVN_BUILD

    for pid in "${!JOBS[@]}"; do
      local cluster_name="${JOBS[$pid]}"
      echo "Waiting for cluster readiness ($cluster_name).."
      if ! wait $pid; then
        echo "################"
        echo "FATAL: cluster  creation (${cluster_name}) (pid: $pid) failed with code ($res). See below partial logs:"
        echo "################"
        tail -17 "${SCRIPT_PATH}/${cluster_name}.log"
        echo "################"
        exit 1
      fi
    done

    local clusters=($CLUSTER_SOURCE $CLUSTER_TARGET)

    mirror_image_to_local_registry $WHEREABOUTS_IMAGE
    mirror_image_to_local_registry $BR_CNI_IMAGE
    for c in ${clusters[@]}; do
      install_whereabouts $c
      install_bridge_cni $c
    done

    for c in ${clusters[@]}; do
      setup_migration_network $c
      wire_ovn_underlay_network $c
    done

    echo "Creating migration network NAD on source cluster ($CLUSTER_SOURCE).."
    client $CLUSTER_SOURCE -n kubevirt apply -f $NAD_MIGRATION_NET_SOURCE
    echo "Creating migration network NAD on target cluster ($CLUSTER_TARGET).."
    client $CLUSTER_TARGET -n kubevirt apply -f $NAD_MIGRATION_NET_TARGET
    
    for c in ${clusters[@]}; do
      echo "($c): patch kubevirt.."
      client $c patch -n kubevirt kubevirt kubevirt --type merge --patch-file $KUBEVIRT_PATCH
    done

    for c in ${clusters[@]}; do
      echo "($c): waiting for pods readiness.."
      client $c wait kubevirt -n kubevirt kubevirt --for condition=Available=true --timeout 20m
      client $c rollout status daemonset/bridge-cni  -n kube-system --timeout=20m
      client $c rollout status daemonset/whereabouts -n kube-system --timeout=20m
    done

    exchange_kubevirt_certificates $CLUSTER_SOURCE_CFG $CLUSTER_TARGET_CFG
}

ovn_cluster_up() {
  local -r name=$1
  local -r kubeconfig=$2
  shift 2
  ./contrib/kind.sh -ep $OCI_BIN -lr -i6 -ds -ml 9 -nl 9 -cl "-vconsole:dbg" -mne -nse -ikv -cn "$name" -kc "$kubeconfig" "$@"
}

mirror_image_to_local_registry() {
  local -r image_tag="$1"
  local -r repo_tag="${image_tag#*/}"
  echo "Mirror image ($image_tag) to local registry.."
  skopeo copy "docker://${image_tag}" "docker://${LOCAL_REGISTRY}/${repo_tag}" --dest-tls-verify=false
}

install_whereabouts() {
  local -r cluster_name="$1"
  echo "($1): Installing Whereabouts CNI plugin using kubeconfig: ${cluster_name}"
  client $cluster_name apply -f $WHEREABOUTS
  client $cluster_name apply -f https://raw.githubusercontent.com/k8snetworkplumbingwg/whereabouts/refs/heads/master/doc/crds/whereabouts.cni.cncf.io_ippools.yaml
  client $cluster_name apply -f https://raw.githubusercontent.com/k8snetworkplumbingwg/whereabouts/refs/heads/master/doc/crds/whereabouts.cni.cncf.io_overlappingrangeipreservations.yaml
}

install_bridge_cni() {
  local cluster_name="$1"
  echo "($1): Installing Bridge CNI plugin using kubeconfig: ${cluster_name}"
  client $cluster_name apply -f $BR_CNI -n kube-system
}

setup_migration_network() {
  echo "($1): Creating migration network (20.100.0.0/24).."
  $OCI_BIN network create --driver bridge --subnet 20.100.0.0/24 $MIGRATION_NETWORK |:
    
  nodes=$(client $1 get no --no-headers -o custom-columns=:.metadata.name)
  for n in $nodes; do 
    echo "($1): node ($n) - connecting to migration network ($MIGRATION_NETWORK)"
    $OCI_BIN network connect $MIGRATION_NETWORK $n |:
    echo "($1): node ($n) - creating migration network bridge ($MIGRATION_BR)"
    $OCI_BIN exec $n ip link add $MIGRATION_BR type bridge  |:
    echo "($1): node ($n) - wire migration network interface to bridge"
    $OCI_BIN exec $n ip link set dev $MIGRATION_IFACE master  $MIGRATION_BR|:
  done 
}

wire_ovn_underlay_network() {
    local -r ovs_pods=$(client $1 -n ovn-kubernetes get pod -l app=ovs-node --no-headers -o custom-columns=:.metadata.name)
    for p in $ovs_pods; do 
        client $1 -n ovn-kubernetes exec $p -- ovs-vsctl add-br $BR_NAME |:
        client $1 -n ovn-kubernetes exec $p -- ovs-vsctl add-port $BR_NAME $UNDERLAY_NIC |:
        client $1 -n ovn-kubernetes exec $p -- ovs-vsctl set open . external_ids:ovn-bridge-mappings="physnet:breth0,$BR_MAPPING:$BR_NAME" |:
    done
}

exchange_kubevirt_certificates() {
    local -r kubeconfig_a="$1"
    local -r kubeconfig_b="$2"

    echo "Exchanging KubeVirt certificates between clusters"
    local -r ca_bundle_a=$(KUBECONFIG="$kubeconfig_a" kubectl get configmap kubevirt-ca -n kubevirt -o jsonpath='{.data.ca-bundle}' 2>/dev/null || echo "")
    if [[ -z "$ca_bundle_a" ]]; then
        echo "Warning: Could not read kubevirt-ca configmap from cluster A, skipping certificate exchange"
        return 1
    fi
    local -r ca_bundle_b=$(KUBECONFIG="$kubeconfig_b" kubectl get configmap kubevirt-ca -n kubevirt -o jsonpath='{.data.ca-bundle}' 2>/dev/null || echo "")
    if [[ -z "$ca_bundle_b" ]]; then
        echo "Warning: Could not read kubevirt-ca configmap from cluster B, skipping certificate exchange"
        return 1
    fi

    echo "Setting cluster B's CA certificate in cluster A's kubevirt-external-ca configmap"
    KUBECONFIG="$kubeconfig_a" kubectl create configmap kubevirt-external-ca -n kubevirt --from-literal=ca-bundle="$ca_bundle_b" --dry-run=client -o yaml | \
        KUBECONFIG="$kubeconfig_a" kubectl apply -f -

    echo "Setting cluster A's CA certificate in cluster B's kubevirt-external-ca configmap"
    KUBECONFIG="$kubeconfig_b" kubectl create configmap kubevirt-external-ca -n kubevirt --from-literal=ca-bundle="$ca_bundle_a" --dry-run=client -o yaml | \
        KUBECONFIG="$kubeconfig_b" kubectl apply -f -

    echo "KubeVirt certificate exchange completed successfully"
}

cluster_down() {
    echo "Deleting source cluster ($CLUSTER_SOURCE).."
    ovn_cluster_up $CLUSTER_SOURCE $CLUSTER_SOURCE_CFG --delete
    echo "Deleting target cluster ($CLUSTER_TARGET).."
    ovn_cluster_up $CLUSTER_TARGET $CLUSTER_TARGET_CFG --delete
}

client() { 
  local -r cluster_name=$1
  shift
  kubectl --kubeconfig="${HOME}/${cluster_name}.conf" "$@"
}

if [ "${$#}" -eq "0" ] ; then
    echo "FATAL: require at least one arg"
    help
fi

options=$(getopt --long "cluster-up,cluster-down,cluster-sync,cluster-sync-target,cluster-sync-source,register-kubevirt,help" --options "h" -- "${@}")
eval set -- "$options"
while true; do
    case "$1" in
    --cluster-up)
        BUILD_OVN_IMAGE=1
        OPT_UP=1
        ;;
    --cluster-down)
        OPT_DOWN=1
        ;;
    --cluster-sync)
        BUILD_OVN_IMAGE=1
        OPT_SOURCE_SYNC=1
        OPT_TARGET_SYNC=1
        ;;
    --cluster-sync-source)
        BUILD_OVN_IMAGE=1
        OPT_SOURCE_SYNC=1
        ;;
    --cluster-sync-target)
        BUILD_OVN_IMAGE=1
        OPT_TARGET_SYNC=1
        ;;
    --register-kubevirt)
        OPT_REGISTER_KUBEVIRT=1
        ;;
    -h | --help)
        help
        exit
        ;;
    --)
        shift
        break
        ;;
    esac
    shift
done

if [ -n "${OPT_DOWN}" ]; then
  cluster_down
fi

if [ -n "${BUILD_OVN_IMAGE}" ]; then
  build_ovn_image
fi

if [ -n "${OPT_UP}" ]; then
  ensure_prerequisites
  configure_kernel_params
  cluster_up
fi

if [ -n "${OPT_SOURCE_SYNC}" ]; then
    export SKIP_OVN_BUILD=true
    ovn_cluster_up $CLUSTER_SOURCE $CLUSTER_SOURCE_CFG --deploy
    unset SKIP_OVN_BUILD
fi

if [ -n "${OPT_TARGET_SYNC}" ]; then
  export SKIP_OVN_BUILD=true
  ovn_cluster_up $CLUSTER_TARGET $CLUSTER_TARGET_CFG --deploy
  unset SKIP_OVN_BUILD
fi

if [ -n "${OPT_REGISTER_KUBEVIRT}" ]; then
  exchange_kubevirt_certificates $CLUSTER_SOURCE_CFG $CLUSTER_TARGET_CFG
fi
