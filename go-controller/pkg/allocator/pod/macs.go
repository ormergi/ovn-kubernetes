package pod

import (
	"fmt"

	kubevirtv1 "kubevirt.io/api/core/v1"

	corev1 "k8s.io/api/core/v1"

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
