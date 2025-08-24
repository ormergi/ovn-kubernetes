package mac

import (
	"errors"
	"net"
	"sync"

	"k8s.io/klog/v2"
)

// store for reserved MAC address request by owner. Key is MAC address, value is owner identifier.
type store map[string]string

// ReservationManager tracks reserved MAC addresses requests of pods on user-defined networks and detect MAC conflicts,
// where one pod request static MAC address that is used by another pod.
// Isolation is maintained, the same MAC address can be used in multiple user-defined networks.
type ReservationManager struct {
	// lock for storing a MAC reservation.
	lock sync.Mutex
	// networkReservations store for user-defined networks MAC reservations. key is network name, value is store.
	networkReservations map[string]store
}

// NewManager creates a new ReservationManager.
func NewManager() *ReservationManager {
	return &ReservationManager{
		networkReservations: make(map[string]store),
	}
}

var ErrMACConflict = errors.New("MAC address already in use")

// Reserve stores the address reservation and its owner in the given network store.
// Returns an error ErrMACConflict in case the given addresses is already reserved on the network by different owner.
func (n *ReservationManager) Reserve(network, owner string, mac net.HardwareAddr) error {
	if network == "" || owner == "" || mac == nil {
		return nil
	}

	n.lock.Lock()
	defer n.lock.Unlock()

	macStore, exists := n.networkReservations[network]
	if !exists {
		macStore = make(store)
		n.networkReservations[network] = macStore
		klog.V(5).Infof("Created MAC reserveation store for network: %s", network)
	}

	macKey := mac.String()
	currentOwner, macReserved := macStore[macKey]
	if macReserved && currentOwner != owner {
		return ErrMACConflict
	}

	if macReserved {
		return nil
	}
	macStore[macKey] = owner
	klog.V(5).Infof("Reserved MAC (%s) for owner (%s) on network (%s)", macKey, owner, network)

	return nil
}

// Release removes MAC address store from given network store.
func (n *ReservationManager) Release(network string, owner string, mac net.HardwareAddr) error {
	if network == "" || owner == "" || mac == nil {
		return nil
	}

	n.lock.Lock()
	defer n.lock.Unlock()

	macStore, exists := n.networkReservations[network]
	if !exists {
		return nil
	}

	macKey := mac.String()
	currentOwner, macReserved := macStore[macKey]
	if !macReserved || currentOwner != owner {
		return nil
	}

	delete(macStore, macKey)
	klog.V(5).Infof("Release MAC (%s) of owner (%s) on network (%s)", macKey, owner, network)

	if len(macStore) == 0 {
		delete(n.networkReservations, network)
		klog.V(5).Infof("Deleted network %s MAC reserveation store", network)
	}

	return nil
}
