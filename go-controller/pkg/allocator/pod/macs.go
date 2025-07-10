package pod

import (
	"fmt"
	"maps"
	"net"

	kubevirtv1 "kubevirt.io/api/core/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	k8snet "k8s.io/utils/net"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

// GetMACOwner compose the owner identifier reserved for MAC addresses management.
// Returns "<ns>/<pod-name>" for regular pods and "<ns>/<vm-name>" for VMs with persistent IPs enabled.
func GetMACOwner(pod *corev1.Pod, netInfo util.NetInfo) string {
	// Check if this is a VM pod and persistent IPs are enabled
	if vmName, ok := pod.Labels[kubevirtv1.VirtualMachineNameLabel]; ok && netInfo.AllowsPersistentIPs() {
		return fmt.Sprintf("%s/%s", pod.Namespace, vmName)
	}

	// Default to pod-based identifier
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}

// ReleasePodReservedMacAddress releases pod's reserved MAC address, if exists.
// It removes the used MAC address, from pod network annotation, and remove it from the MAC manager store.
func (allocator *PodAnnotationAllocator) ReleasePodReservedMacAddress(pod *corev1.Pod, nadName string) error {
	podNetworks, err := util.UnmarshalPodAnnotationAllNetworks(pod.Annotations)
	if err != nil {
		return fmt.Errorf("failed to unmarshal pod annotation: %w", err)
	}
	for nad, podNetwork := range podNetworks {
		if nad != nadName || podNetwork.MAC == "" {
			continue
		}
		mac, perr := net.ParseMAC(podNetwork.MAC)
		if perr != nil {
			return fmt.Errorf("failed to parse MAC address from pod annotation: %v", perr)
		}
		networkName := allocator.netInfo.GetNetworkName()
		owner := GetMACOwner(pod, allocator.netInfo)
		if aerr := allocator.macManager.Release(networkName, owner, mac); aerr != nil {
			// avoid exposing network name in error because they may reflect on pod event
			return fmt.Errorf("failed to release MAC address (%s) for owner (%s) on network attachment (%s): %w",
				podNetwork.MAC, owner, nad, aerr)
		}

		klog.V(5).Infof("Released MAC: (%s), pod: (%s/%s), network: (%s), nad: (%s)",
			podNetwork.MAC, pod.Namespace, pod.Name, networkName, nad)
	}

	return nil
}

// InitializeMACManager initializes MAC reservation tracker with MAC addresses in use in the network.
func (allocator *PodAnnotationAllocator) InitializeMACManager() error {
	macs := calculateSubnetsInfraMACAddresses(allocator.netInfo.Subnets())

	pods, err := allocator.fetchNetworkPods()
	if err != nil {
		return err
	}
	podMACs, err := allocator.getPodMACs(pods)
	if err != nil {
		return err
	}
	maps.Copy(macs, podMACs)

	networkName := allocator.netInfo.GetNetworkName()
	for owner, mac := range macs {
		if rerr := allocator.macManager.Reserve(networkName, owner, mac); rerr != nil {
			return fmt.Errorf("failed to reserve MAC (%s) for owner (%s) on network (%s): %w",
				mac, owner, networkName, rerr)
		}
	}

	return nil
}

// calculateSubnetsInfraMACAddresses return map of the network infrastructure mac addresses and owner name.
// It calculates the gateway (.2) and management (.1) ports MAC addresses from their IP address.
func calculateSubnetsInfraMACAddresses(subnets []config.CIDRNetworkEntry) map[string]net.HardwareAddr {
	reservedMACs := map[string]net.HardwareAddr{}
	for _, subnet := range subnets {
		if subnet.CIDR == nil {
			continue
		}

		gwIP := util.GetNodeGatewayIfAddr(subnet.CIDR)
		gwMAC := util.IPAddrToHWAddr(gwIP.IP)
		gwKey := fmt.Sprintf("gw-v%s", k8snet.IPFamilyOf(gwIP.IP))
		reservedMACs[gwKey] = gwMAC

		mgmtIP := util.GetNodeManagementIfAddr(subnet.CIDR)
		mgmtMAC := util.IPAddrToHWAddr(mgmtIP.IP)
		mgmtKey := fmt.Sprintf("mgmt-v%s", k8snet.IPFamilyOf(mgmtIP.IP))
		reservedMACs[mgmtKey] = mgmtMAC
	}

	return reservedMACs
}

// fetchNetworkPods fetch pods in to the network NAD namespaces.
func (allocator *PodAnnotationAllocator) fetchNetworkPods() ([]*corev1.Pod, error) {
	var netPods []*corev1.Pod
	for _, ns := range allocator.netInfo.GetNADNamespaces() {
		pods, err := allocator.podLister.Pods(ns).List(labels.Everything())
		if err != nil {
			return nil, fmt.Errorf("failed to list pods for namespace %q: %v", ns, err)
		}
		for _, pod := range pods {
			if pod == nil {
				continue
			}
			if pod.Status.Phase != corev1.PodRunning || !pod.DeletionTimestamp.IsZero() && len(pod.Finalizers) == 0 {
				// skip pods who are non-running or about to dispose
				continue
			}
			netPods = append(netPods, pod)
		}
	}
	return netPods, nil
}

// getPodMACs collected the given pods used MAC addresses from the pod-network annotation.
func (allocator *PodAnnotationAllocator) getPodMACs(pods []*corev1.Pod) (map[string]net.HardwareAddr, error) {
	podMACs := map[string]net.HardwareAddr{}
	for _, pod := range pods {
		podNetworks, err := util.UnmarshalPodAnnotationAllNetworks(pod.Annotations)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal pod annotations %s/%s: %v", pod.Namespace, pod.Name, err)
		}
		for _, network := range podNetworks {
			if network.Role != types.NetworkRoleInfrastructure {
				// primary UDN network role is infrastructure-lock on primary UDNs only.
				continue
			}
			mac, perr := net.ParseMAC(network.MAC)
			if perr != nil {
				return nil, fmt.Errorf("failed to parse mac address %s/%s: %v", pod.Namespace, pod.Name, perr)
			}
			podMACs[GetMACOwner(pod, allocator.netInfo)] = mac
		}
	}

	return podMACs, nil
}
