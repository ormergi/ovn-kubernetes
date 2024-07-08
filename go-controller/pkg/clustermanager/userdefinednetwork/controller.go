package userdefinednetwork

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	netv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netv1clientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"
	netv1infomer "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions/k8s.cni.cncf.io/v1"
	netv1lister "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"

	userdefinednetworkv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1"
	udnapplyconfkv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/applyconfiguration/userdefinednetwork/v1"
	userdefinednetworkclientset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/clientset/versioned"
	userdefinednetworkinformer "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/informers/externalversions/userdefinednetwork/v1"
	userdefinednetworklister "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/listers/userdefinednetwork/v1"

	nadnotifier "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/clustermanager/userdefinednetwork/notifier"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/controller"
)

var udnGroupVersionKind = userdefinednetworkv1.SchemeGroupVersion.WithKind("UserDefinedNetwork")

type nadRenderer interface {
	RenderNetAttachDefManifest(*userdefinednetworkv1.UserDefinedNetwork) (*netv1.NetworkAttachmentDefinition, error)
}

type Controller struct {
	lock sync.Mutex

	Controller controller.Controller

	udnClient userdefinednetworkclientset.Interface
	udnLister userdefinednetworklister.UserDefinedNetworkLister

	nadNotifier *nadnotifier.NetAttachDefNotifier
	nadClient   netv1clientset.Interface
	nadLister   netv1lister.NetworkAttachmentDefinitionLister

	nadRenderer nadRenderer
}

func New(
	nadClient netv1clientset.Interface,
	nadInfomer netv1infomer.NetworkAttachmentDefinitionInformer,
	udnClient userdefinednetworkclientset.Interface,
	udnInformer userdefinednetworkinformer.UserDefinedNetworkInformer,
	nadRenderer nadRenderer,
) *Controller {
	udnLister := udnInformer.Lister()
	c := &Controller{
		nadClient:   nadClient,
		nadLister:   nadInfomer.Lister(),
		udnClient:   udnClient,
		udnLister:   udnLister,
		nadRenderer: nadRenderer,
	}
	cfg := &controller.ControllerConfig[userdefinednetworkv1.UserDefinedNetwork]{
		RateLimiter:    workqueue.DefaultControllerRateLimiter(),
		Reconcile:      c.UserDefinedNetworkReconciler,
		ObjNeedsUpdate: c.udnNeedUpdate,
		Threadiness:    1,
		Informer:       udnInformer.Informer(),
		Lister:         udnLister.List,
	}
	c.Controller = controller.NewController[userdefinednetworkv1.UserDefinedNetwork]("user-defined-network-controller", cfg)

	c.nadNotifier = nadnotifier.NewNetAttachDefNotifier(nadInfomer, c)

	return c
}

func (c *Controller) ReconcileNetAttachDef(key string) {
	// enqueue network-attachment-definitions requests in the controller workqueue
	c.Controller.Reconcile(key)
}

func (c *Controller) Run() error {
	klog.Infof("Starting UserDefinedNetworkManager Controllers")
	if err := controller.Start(c.nadNotifier.Controller, c.Controller); err != nil {
		return fmt.Errorf("unable to start UserDefinedNetworkManager controller: %v", err)
	}

	return nil
}

func (c *Controller) Shutdown() {
	controller.Stop(c.nadNotifier.Controller, c.Controller)
}

func (c *Controller) udnNeedUpdate(_, _ *userdefinednetworkv1.UserDefinedNetwork) bool {
	return true
}

// UserDefinedNetworkReconciler get the user-defined-network CRD instance key and reconcile it according to spec.
// It creates network-attachment-definition according to spec at the namespace the UDN object resides.
// The NAD object are created with the same key as the request NAD, having both kinds have the same key enable
// the controller to act on NAD changes as well and reconciles NAD objects (e.g: in case NAD is deleted it will be re-created).
func (c *Controller) UserDefinedNetworkReconciler(key string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	udn, err := c.udnLister.UserDefinedNetworks(namespace).Get(name)
	if err != nil && !kerrors.IsNotFound(err) {
		return fmt.Errorf("failed to get UserDefinedNetwork %q from cache: %v", key, err)
	}

	nad, err := c.nadLister.NetworkAttachmentDefinitions(namespace).Get(name)
	if err != nil && !kerrors.IsNotFound(err) {
		return fmt.Errorf("failed to get NetworkAttachmentDefinition %q from cache: %v", key, err)
	}

	udnCopy := udn.DeepCopy()
	nadCopy := nad.DeepCopy()

	nadCopy, syncErr := c.SyncUserDefinedNetwork(udnCopy, nadCopy)

	updateStatusErr := c.UpdateUserDefinedNetworkStatus(udnCopy, nadCopy, syncErr)

	return errors.Join(syncErr, updateStatusErr)
}

func (c *Controller) SyncUserDefinedNetwork(udn *userdefinednetworkv1.UserDefinedNetwork, nad *netv1.NetworkAttachmentDefinition) (*netv1.NetworkAttachmentDefinition, error) {
	if udn == nil {
		return nil, nil
	}

	desiredNAD, err := c.nadRenderer.RenderNetAttachDefManifest(udn)
	if err != nil {
		return nil, fmt.Errorf("failed to generate NetworkAttachmetDefinition: %w", err)
	}
	if nad == nil {
		nad, err = c.nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), desiredNAD, metav1.CreateOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create NetworkAttachmetDefinition: %w", err)
		}
		klog.Infof("Created NetworkAttachmentDefinition [%s/%s]", nad.Namespace, nad.Name)
		return nad, nil
	}

	if !ownedByUserDefinedNetwork(nad, udnGroupVersionKind, udn.UID) {
		return nil, fmt.Errorf("foreign NetworkAttachmetDefinition with the desired name already exist")
	}

	if !reflect.DeepEqual(nad.Spec, desiredNAD.Spec) ||
		!reflect.DeepEqual(nad.ObjectMeta.OwnerReferences, desiredNAD.ObjectMeta.OwnerReferences) {
		nad.Spec.Config = desiredNAD.Spec.Config
		nad.ObjectMeta.OwnerReferences = desiredNAD.ObjectMeta.OwnerReferences
		nad, err = c.nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(nad.Namespace).Update(context.Background(), nad, metav1.UpdateOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to update NetworkAttachmetDefinition: %w", err)
		}
		klog.Infof("Updated NetworkAttachmetDefinition [%s/%s]", nad.Namespace, nad.Name)
	}

	return nad, nil
}

func ownedByUserDefinedNetwork(nad *netv1.NetworkAttachmentDefinition, gvk schema.GroupVersionKind, uid types.UID) bool {
	for _, ownerRef := range nad.OwnerReferences {
		if ownerRef.UID == uid && ownerRef.Kind == gvk.Kind && ownerRef.APIVersion == gvk.GroupVersion().String() {
			return true
		}
	}
	return false
}

func (c *Controller) UpdateUserDefinedNetworkStatus(udn *userdefinednetworkv1.UserDefinedNetwork, nad *netv1.NetworkAttachmentDefinition, syncError error) error {
	if udn == nil {
		return nil
	}

	networkReadyCondition := newNetworkReadyCondition(nad, syncError)

	var updatedConditions []metav1.Condition
	if len(udn.Status.Conditions) == 0 {
		updatedConditions = append(updatedConditions, *networkReadyCondition)
	} else {
		cp := slices.Clone(udn.Status.Conditions)
		updatedConditions = updateCondition(cp, networkReadyCondition)
	}

	if !reflect.DeepEqual(udn.Status.Conditions, updatedConditions) {
		var err error
		udnApplyConf := udnapplyconfkv1.UserDefinedNetwork(udn.Name, udn.Namespace).
			WithStatus(udnapplyconfkv1.UserDefinedNetworkStatus().
				WithConditions(updatedConditions...))
		opts := metav1.ApplyOptions{FieldManager: "user-defined-network-controller"}
		udn, err = c.udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).ApplyStatus(context.Background(), udnApplyConf, opts)
		if err != nil {
			return fmt.Errorf("failed to update UserDefinedNetwork status: %w", err)
		}
		klog.Infof("Updated status UserDefinedNetwork [%s/%s]", udn.Namespace, udn.Name)
	}

	return nil
}

func newNetworkReadyCondition(nad *netv1.NetworkAttachmentDefinition, syncError error) *metav1.Condition {
	now := metav1.Now()
	networkReadyCondition := &metav1.Condition{
		Type:               "NetworkReady",
		Status:             metav1.ConditionTrue,
		Reason:             "NetworkAttachmentDefinitionReady",
		Message:            "NetworkAttachmentDefinition has been created",
		LastTransitionTime: now,
	}

	if nad == nil {
		networkReadyCondition.Status = metav1.ConditionFalse
		networkReadyCondition.Reason = "NetworkAttachmentDefinitionNotExist"
		networkReadyCondition.Message = "NetworkAttachmentDefinition is not exist"
	} else if !nad.DeletionTimestamp.IsZero() {
		networkReadyCondition.Status = metav1.ConditionFalse
		networkReadyCondition.Reason = "NetworkAttachmentDefinitionDeleted"
		networkReadyCondition.Message = "NetworkAttachmentDefinition is being deleted"
	}
	if syncError != nil {
		networkReadyCondition.Status = metav1.ConditionFalse
		networkReadyCondition.Reason = "SyncError"
		networkReadyCondition.Message = syncError.Error()
	}

	return networkReadyCondition
}

func updateCondition(conditions []metav1.Condition, cond *metav1.Condition) []metav1.Condition {
	idx := slices.IndexFunc(conditions, func(c metav1.Condition) bool {
		return (c.Type == cond.Type) &&
			(c.Status != cond.Status || c.Reason != cond.Reason || c.Message != cond.Message)
	})
	if idx != -1 {
		return slices.Replace(conditions, idx, idx+1, *cond)
	}
	return conditions
}
