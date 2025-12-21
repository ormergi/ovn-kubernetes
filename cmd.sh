#! /bin/bash 

set -e

# spike cross cluster migration over localnet issues.sh
# https://github.com/ovn-kubernetes/ovn-kubernetes/blob/master/test/e2e/kubevirt.go

CRI_BIN=${CRI_BIN:-podman}

BR_MAPPING="localnet1"
BR_NAME="ovsbr1"
UNDERLAY_NIC="eth1"
CUDN="localnet1"

CLUSTER_1="ovn1"
CLUSTER_2="ovn2"
CLUSTER_1_CONF="${HOME}/${CLUSTER_1}.conf"
CLUSTER_2_CONF="${HOME}/${CLUSTER_2}.conf"

NAMESPACE="blue"

KUBEVIRT_VERSION="v1.7.0-beta.0"

VM_IMAGE_US="quay.io/kubevirt/fedora-with-test-tooling-container-disk:v1.6.2"
VM_IMAGE="localhost:5000/kubevirt/fedora-with-test-tooling-container-disk:v1.6.2"
VM_IMAGE_LOCAL="localhost:5000/kubevirt/fedora-with-test-tooling-container-disk:v1.6.2"

VM_NAME_DST="testvmi-dst"
VM_NAME_SRC="testvmi-src"

MIGRATION_NETWORK="migration"
MIGRATION_NAD="migration-br"
MIGRATION_BR="br-mig"
MIGRATION_IFACE="eth2"

VMIM_DST="testvmi-dst"

alias k1="kubectl --kubeconfig $CLUSTER_1_CONF"
alias k2="kubectl --kubeconfig $CLUSTER_2_CONF"
alias kv1="kubectl --kubeconfig $CLUSTER_1_CONF -n kubevirt"
alias kv2="kubectl --kubeconfig $CLUSTER_2_CONF -n kubevirt"
alias kovn1="kubectl --kubeconfig $CLUSTER_1_CONF -n ovn-kubernetes"
alias kovn2="kubectl --kubeconfig $CLUSTER_2_CONF -n ovn-kubernetes"

dependencies() {
  #requirements=(pip3 openssl wget virtctl)
  #pip_reqs=(jinjanator)

  if ! which wget &> /dev/null; then
    dnf install wget
  fi 

  if ! which openssl &> /dev/null; then
    dnf install openssl
  fi 
  
  if ! which pip3 &> /dev/null; then
    dnf install pip3
  fi 

  if ! which jinjanate &> /dev/null; then
    pip3 install jinjanator
  fi 

  if ! which virtctl &> /dev/null; then 
    echo "install virtctl"
    v=$(curl https://storage.googleapis.com/kubevirt-prow/release/kubevirt/kubevirt/stable.txt)
    wget https://github.com/kubevirt/kubevirt/releases/download/${v}/virtctl-${v}-linux-amd64
    chmod +x "virtctl-$v-linux-amd64"
    cp virtctl-$v-linux-amd64 /usr/bin/virtctl
  fi 
}

up() {
    sysctl -w fs.inotify.max_user_instances=1024

    export KUBEVIRT_VERSION

    declare -A JOBS
    if [[ ! $(kind get clusters -q) =~ $CLUSTER_1 ]]; then
      echo "creating cluster $CLUSTER_1.."
      ./contrib/kind.sh -ep $CRI_BIN -lr -i6 -ds -mne -nse -ikv -cn $CLUSTER_1 -kc $CLUSTER_1_CONF &> "${CLUSTER_1}.log" &
      JOBS["$!"]="$CLUSTER_1"
    fi

    if [[ ! $(kind get clusters -q) =~ $CLUSTER_2 ]]; then
      sleep 10
      echo "creating cluster $CLUSTER_2.."
      ./contrib/kind.sh -ep $CRI_BIN -lr -i6 -ds -mne -nse -ikv -cn $CLUSTER_2 -kc $CLUSTER_2_CONF &> "${CLUSTER_2}.log" &
      JOBS["$!"]="$CLUSTER_2"
    fi
    
    clusters=()
    for pid in "${!JOBS[@]}"; do
      cluster_name="${JOBS[$pid]}"
      echo "waiting for cluster ($cluster_name)..."      
      if ! wait $pid; then 
        echo "################"
        echo "FATAL: cluster  creation (${cluster_name}) (pid: $pid) failed with code ($res). See below partial logs:"
        echo "################"
        tail -17 "${cluster_name}.log"
        echo "################"
        exit 1
      fi
      clusters+=($cluster_name)
    done

    whereabouts_img="k8snetworkplumbingwg/whereabouts:latest"
    echo "Mirror $whereabouts_img to local registry.."
    skopeo copy "docker://ghcr.io/${whereabouts_img}" "docker://localhost:5000/${whereabouts_img}" --dest-tls-verify=false
    brcni_img="kubevirt/cni-default-plugins:v1.5.1"
    echo "Mirror $brcni_img to local registry.."
    skopeo copy "docker://quay.io/${brcni_img}" "docker://localhost:5000/${brcni_img}" --dest-tls-verify=false

    for c in ${clusters[@]}; do
      install_bridge_cni $c
      install_whereabouts $c
    done 

    for c in ${clusters[@]}; do
      setup_migration_network $c
      setup_underlay_network $c
    done

    echo "Creating migration network NAD on source cluster ($CLUSTER_1).."
    client $CLUSTER_1 -n kubevirt apply -f ./nad-migration-src
    echo "Creating migration network NAD on destination cluster ($CLUSTER_2).."
    client $CLUSTER_2 -n kubevirt apply -f ./nad-migration-dst
    
    for c in ${clusters[@]}; do
      echo "cluster ($c) - patch kubevirt.."
      client $c patch -n kubevirt kubevirt kubevirt --type merge --patch-file ./kv-patch 
    done

    
    for c in ${clusters[@]}; do
      echo "wating for pods readiness ($c)..."
      client $c wait kubevirt -n kubevirt kubevirt --for condition=Available=true --timeout 20m
      client $c rollout status daemonset/bridge-cni  -n kube-system --timeout=20m
      client $c rollout status daemonset/whereabouts -n kube-system --timeout=20m
    done

    exchange_kubevirt_certificates $CLUSTER_1_CONF $CLUSTER_2_CONF
}

setup_migration_network() {
  echo "create migration network (20.100.0.0/24).."
  $CRI_BIN network create --driver bridge --subnet 20.100.0.0/24 $MIGRATION_NETWORK |:
    
  nodes=$(client $1 get no --no-headers -o custom-columns=:.metadata.name)
  for n in $nodes; do 
    echo "cluster ($1) node ($n) - connecting to migration network ($MIGRATION_NETWORK)"
    $CRI_BIN network connect $MIGRATION_NETWORK $n |:
    echo "cluster ($1) node ($n) - creating migration network bridge ($MIGRATION_BR)"
    $CRI_BIN exec $n ip link add $MIGRATION_BR type bridge  |:
    echo "cluster ($1) node ($n) - wire migration network interface to bridge"
    $CRI_BIN exec $n ip link set dev $MIGRATION_IFACE master  $MIGRATION_BR|:
  done 
}

setup_underlay_network() {
    local -r ovs_pods=$(client $1 -n ovn-kubernetes get pod -l app=ovs-node --no-headers -o custom-columns=:.metadata.name)
    for p in $ovs_pods; do 
        client $1 -n ovn-kubernetes exec $p -- ovs-vsctl add-br $BR_NAME |:
        client $1 -n ovn-kubernetes exec $p -- ovs-vsctl add-port $BR_NAME $UNDERLAY_NIC |:
        client $1 -n ovn-kubernetes exec $p -- ovs-vsctl set open . external_ids:ovn-bridge-mappings="physnet:breth0,$BR_MAPPING:$BR_NAME" |:
    done
}

install_whereabouts() {
  local cluster_name="$1"
  echo "Installing Whereabouts CNI plugin using kubeconfig: ${cluster_name}"
  client $cluster_name apply -f ./whereaboutscni
  client $cluster_name apply -f https://raw.githubusercontent.com/k8snetworkplumbingwg/whereabouts/refs/heads/master/doc/crds/whereabouts.cni.cncf.io_ippools.yaml
  client $cluster_name apply -f https://raw.githubusercontent.com/k8snetworkplumbingwg/whereabouts/refs/heads/master/doc/crds/whereabouts.cni.cncf.io_overlappingrangeipreservations.yaml
}

install_bridge_cni() {
  local cluster_name="$1"
  echo "Installing Bridge CNI plugin using kubeconfig: ${cluster_name}"
  client $cluster_name apply -f ./brcni -n kube-system
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

down() {
    echo "deleting cluster $CLUSTER_1.."
    (./contrib/kind.sh -ep $CRI_BIN -lr -i6 -ds -mne -nse -ikv -cn $CLUSTER_1 -kc $CLUSTER_1_CONF --delete)
    echo "deleting cluster $CLUSTER_2.."
    (./contrib/kind.sh -ep $CRI_BIN -lr -i6 -ds -mne -nse -ikv -cn $CLUSTER_2 -kc $CLUSTER_2_CONF --delete)
}

teardown_underlay() {
    local -r ovs_pods=$(client $1 -n ovn-kubernetes get pod -l app=ovs-node --no-headers -o custom-columns=:.metadata.name)
    for p in $ovs_pods; do 
        client $1 -n ovn-kubernetes exec $p -- ovs-vsctl del-port $BR_NAME $UNDERLAY_NIC
        client $1 -n ovn-kubernetes exec $p -- ovs-vsctl del-br $BR_NAME
        client $1 -n ovn-kubernetes exec $p -- ovs-vsctl set open . external_ids:ovn-bridge-mappings="physnet:breth0"
    done
}

test() {
    echo "creating VM on cluster 1 (src).."
    client $CLUSTER_1 -n $NAMESPACE apply -f ./vm-src
    echo "creating VM on cluster 2 (dest).."
    client $CLUSTER_2 -n $NAMESPACE apply -f ./vm-dst
    echo "wating for src VM readiness.."
    client $CLUSTER_1 -n $NAMESPACE wait vm $VM_NAME_SRC --for condition=AgentConnected --timeout=5m

    $CRI_BIN run --privileged --network underlay -d -it ghcr.io/nicolaka/netshoot:v0.14 sleep 30000
    $CRI_BIN exec ip addr add 192.168.1.150/24 dev eth0
    $CRI_BIN exec iperf3 -s 

    virtctl --kubeconfig=$CLUSTER_1_CONF console -n $NAMESPACE $VM_NAME_SRC 

    cluster_2_url="$(client $CLUSTER_2 get kubevirt -n kubevirt kubevirt -o jsonpath='{.status.synchronizationAddresses}' | jq .[] -r)"
    migration_id="test-cclm"
    echo "creating VMIM on source cluster ($CLUSTER_1)"
    create_vmim_src $CLUSTER_1_CONF $migration_id $cluster_2_url
    echo "creating VMIM on destination cluster ($CLUSTER_2)"
    create_vmim_dst $CLUSTER_2_CONF $migration_id 
    echo "waiting for migration to complete..."
    client $CLUSTER_2 -n $NAMESPACE wait  --for=jsonpath='{.status.phase}'=Succeeded vmim/$VMIM_DST --timeout=7m 

    client $CLUSTER_2 client $CLUSTER_2 get vmim $VMIM_DST -o yaml
    
    client $CLUSTER_1 client get vm 
    client $CLUSTER_2 client get vm 

    read -p "continue?"
}

create_vmim_src() {
    local -r cluster_conf=$1
    local -r id=$2
    local -r url=$3
    kubectl --kubeconfig="${cluster_conf}" apply -n $NAMESPACE -f - <<EOF
apiVersion: kubevirt.io/v1
kind: VirtualMachineInstanceMigration
metadata:
  name: testvmi-src
spec:
  vmiName: testvmi-src
  sendTo:
    connectURL: "${url}"
    migrationID: "${id}" 
EOF
}

# aka target
create_vmim_dst() {
    local -r cluster_conf=$1
    local -r id=$2
    kubectl --kubeconfig="${cluster_conf}" -n $NAMESPACE apply -f - <<EOF
apiVersion: kubevirt.io/v1
kind: VirtualMachineInstanceMigration
metadata:
  name: testvmi-dst
spec:
  vmiName: testvmi-dst
  receive:
    migrationID: "${id}"
EOF
}

test_setup() {
    client $1 create namespace $NAMESPACE
    create_cudn_localnet $1
}

create_cudn_localnet() {
    client $1 apply -f - <<EOF
apiVersion: k8s.ovn.org/v1
kind: ClusterUserDefinedNetwork
metadata:
  name: $CUDN
spec:
  namespaceSelector:
    matchExpressions:
      - key: kubernetes.io/metadata.name
        operator: In
        values: ["$NAMESPACE"]
  network:
    topology: Localnet
    localnet:
      role: Secondary
      physicalNetworkName: $BR_MAPPING
      ipam: {mode: Disabled}
EOF
}

test_teardown() {
  client $1 delete namespace $NAMESPACE
  client $1 delete clusteruserdefinednetwork $CUDN
}

client() { 
  local -r cluster_name=$1
  shift
  kubectl --kubeconfig="${HOME}/${cluster_name}.conf" "$@"
}

# down 

# up

# VM_IMAGE_US="quay.io/kubevirtci/fedora-with-test-tooling:v20250416-e37573e"
# VM_IMAGE_LOCAL="localhost:5000/kubevirtci/fedora-with-test-tooling:v20250416-e37573e"
# skopeo copy docker://${VM_IMAGE_US} docker://${VM_IMAGE_LOCAL} --dest-tls-verify=false

# podman save quay.io/sronanrh/iperf:latest -o img
# kind load image-archive --name ovn1 ./img
# kind load image-archive --name ovn2 ./img

(
  export IPERF3_IMAGE="localhost:5000/sronanrh/iperf:latest"  
  export TEST_REPORT_DIR="./artifacts/"
  export FLAKE_ATTEMPTS=0
  export KUBECONFIG=$(realpath $CLUSTER_1_CONF)
  export SRC_CLUSTER_CONF=$(realpath $CLUSTER_1_CONF) 
  export DST_CLUSTER_CONF=$(realpath $CLUSTER_2_CONF)
  export CONTAINER_RUNTIME=$CRI_BIN 
  export PLATFORM_IPV4_SUPPORT=true PLATFORM_IPV6_SUPPORT=true
  export ENABLE_MULTI_NET=true
  export ENABLE_NETWORK_SEGMENTATION=true
  export WHAT="Kubevirt Virtual Machines" 
  make -C test control-plane
)