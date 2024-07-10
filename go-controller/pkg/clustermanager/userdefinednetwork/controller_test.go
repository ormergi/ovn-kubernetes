package userdefinednetwork

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	informerfactory "k8s.io/client-go/informers"
	corev1informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/testing"
	"k8s.io/utils/pointer"

	netv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netv1fakeclientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned/fake"
	netv1informerfactory "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions"
	netv1Informer "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions/k8s.cni.cncf.io/v1"

	udnv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1"
	udnfakeclient "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/clientset/versioned/fake"
	udninformerfactory "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/informers/externalversions"
	udninformer "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/informers/externalversions/userdefinednetwork/v1"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

var _ = Describe("User Defined Network Controller", func() {
	var (
		udnClient   *udnfakeclient.Clientset
		nadClient   *netv1fakeclientset.Clientset
		udnInformer udninformer.UserDefinedNetworkInformer
		nadInformer netv1Informer.NetworkAttachmentDefinitionInformer
		podInformer corev1informer.PodInformer
	)

	BeforeEach(func() {
		udnClient = udnfakeclient.NewSimpleClientset()
		udnInformer = udninformerfactory.NewSharedInformerFactory(udnClient, 15).K8s().V1().UserDefinedNetworks()
		nadClient = netv1fakeclientset.NewSimpleClientset()
		nadInformer = netv1informerfactory.NewSharedInformerFactory(nadClient, 15).K8sCniCncfIo().V1().NetworkAttachmentDefinitions()
		kubeClient := fake.NewSimpleClientset()
		sharedInformer := informerfactory.NewSharedInformerFactoryWithOptions(kubeClient, 15)
		podInformer = sharedInformer.Core().V1().Pods()
	})

	Context("reconciler", func() {
		It("should succeed", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			Expect(c.UserDefinedNetworkReconciler("test/test")).To(Succeed())
		})
		It("should fail when parsing key fails", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			Expect(c.UserDefinedNetworkReconciler("a//a")).ToNot(Succeed())
		})
	})

	Context("UserDefinedNetwork object sync", func() {
		It("should create NAD successfully", func() {
			nad := testNAD()
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(nad), nil)

			udn := testUDN()
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, nil)
			Expect(err).ToNot(HaveOccurred())

			actualNAD, err := nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Get(context.Background(), udn.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(actualNAD).To(Equal(nad))
		})

		It("should fail when NAD renderer fails", func() {
			expectedError := errors.New("render error")
			c := New(nadClient, nadInformer, udnClient, udnInformer, failRenderNadStub(expectedError), nil)

			udn := testUDN()
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(expectedError))

			_, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Get(context.Background(), udn.Name, metav1.GetOptions{})
			Expect(kerrors.IsNotFound(err)).To(BeTrue(), "should be not-found error")
		})
		It("should fail when NAD creation fails", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(testNAD()), nil)

			expectedError := errors.New("create NAD error")
			nadClient.PrependReactor("create", "network-attachment-definitions", func(action testing.Action) (handled bool, ret runtime.Object, err error) {
				return true, nil, expectedError
			})

			udn := testUDN()
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, nil)
			Expect(err).To(MatchError(expectedError), "should fail due to client error")
		})

		It("should fail when foreign NAD exist (foreign NAD - same name, not created by the controller)", func() {
			udn := testUDN()
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			nad := testNAD()
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(nad), nil)

			foreignNAD := nad.DeepCopy()
			foreignNAD.OwnerReferences = nil
			foreignNAD, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), foreignNAD, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, foreignNAD)
			Expect(err).To(Equal(errors.New("foreign NetworkAttachmetDefinition with the desired name already exist")))
		})
		It("should reconcile mutated NAD", func() {
			udn := testUDN()
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			nad := testNAD()
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(nad), nil)
			nad.Spec.Config = "NETCONF"

			mutetedNAD := nad.DeepCopy()
			mutetedNAD.Spec.Config = "MUTATED"
			mutetedNAD, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), mutetedNAD, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, mutetedNAD)

			Expect(mutetedNAD).To(Equal(nad))
		})
		It("should fail when updating mutated NAD fails", func() {
			udn := testUDN()
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			nad := testNAD()
			nad.Spec.Config = "NETCONF"
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(nad), nil)

			mutetedNAD := nad.DeepCopy()
			mutetedNAD.Spec.Config = "MUTATED"
			mutetedNAD, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), mutetedNAD, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			expectedErr := errors.New("update error")
			nadClient.PrependReactor("update", "network-attachment-definitions", func(action testing.Action) (bool, runtime.Object, error) {
				return true, nil, expectedErr
			})

			_, err = c.SyncUserDefinedNetwork(udn, mutetedNAD)

			Expect(err).To(MatchError(expectedErr))
		})

		It("should fail when NAD owner-reference is malformed", func() {
			udn := testUDN()
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			nad := testNAD()
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(nad), nil)

			mutetedNAD := nad.DeepCopy()
			mutetedNAD.ObjectMeta.OwnerReferences = []metav1.OwnerReference{{Kind: "DifferentKind"}}
			mutetedNAD, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), mutetedNAD, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, mutetedNAD)
			Expect(err).To(Equal(errors.New("foreign NetworkAttachmetDefinition with the desired name already exist")))
		})

		It("UDN with role primary, should fail when primary network NAD already exist", func() {
			udn := testUDN()
			udn.Spec.Role = udnv1.NetworkRolePrimary
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(testNAD()), nil)

			primaryNet := primaryNetNAD()
			Expect(nadInformer.Informer().GetIndexer().Add(primaryNet)).To(Succeed())

			_, err := c.SyncUserDefinedNetwork(udn, nil)
			Expect(err).To(MatchError(`primary network already exist in namespace "test": "primary-net-1"`))
		})
		It("UDN with role primary, should fail when unmarshaling existing primary network NAD fails", func() {
			udn := testUDN()
			udn.Spec.Role = udnv1.NetworkRolePrimary
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(testNAD()), nil)

			primaryNet := primaryNetNAD()
			primaryNet.Spec.Config = "!@#"
			Expect(nadInformer.Informer().GetIndexer().Add(primaryNet)).To(Succeed())

			_, err := c.SyncUserDefinedNetwork(udn, nil)
			Expect(err.Error()).To(ContainSubstring(`failed to validate no primary network exist: unmarshal failed [test/primary-net-1]`))
		})

		It("should add finalizer to UDN if not exist", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(testNAD()), podInformer)

			udn := testUDN()
			udn.Finalizers = nil
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(udn.Finalizers).To(Equal([]string{"k8s.ovn.org/user-defined-network-protection"}))
		})
		It("should fail adding finalizer to UDN when patch fails", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			udn := testUDN()
			udn.Finalizers = nil
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			expectedErr := errors.New("patch error")
			udnClient.PrependReactor("patch", "userdefinednetworks", func(action testing.Action) (bool, runtime.Object, error) {
				return true, nil, expectedErr
			})

			_, err = c.SyncUserDefinedNetwork(udn, nil)
			Expect(err).To(MatchError(expectedErr))
		})
		It("when udn is being deleted, should not remove finalizer from non managed NAD", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			udn := testsUDNWithDeletionTimestamp(time.Now())
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			unmanagedNAD := testNAD()
			unmanagedNAD.OwnerReferences[0].UID = "99"
			unmanagedNAD, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), unmanagedNAD, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, unmanagedNAD)
			Expect(err).ToNot(HaveOccurred())

			expectedFinalizers := testNAD().Finalizers

			Expect(unmanagedNAD.Finalizers).To(Equal(expectedFinalizers))
		})
		It("when udn is being deleted, and NAD exist, should remove finalizer from NAD", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			udn := testsUDNWithDeletionTimestamp(time.Now())
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			nad := testNAD()
			nad, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), nad, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, nad)
			Expect(err).ToNot(HaveOccurred())
			Expect(nad.Finalizers).To(BeEmpty())
		})
		It("when udn is being deleted, and NAD exist, should fail remove NAD finalizer when patch fails", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			udn := testsUDNWithDeletionTimestamp(time.Now())
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			nad := testNAD()
			nad, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), nad, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			expectedErr := errors.New("patch NAD error")
			nadClient.PrependReactor("patch", "network-attachment-definitions", func(action testing.Action) (bool, runtime.Object, error) {
				return true, nil, expectedErr
			})

			_, err = c.SyncUserDefinedNetwork(udn, nad)
			Expect(err).To(MatchError(expectedErr))
		})

		It("when udn is being deleted, and NAD exist w/o finalizer, should remove finalizer from UDN", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			udn := testsUDNWithDeletionTimestamp(time.Now())
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			nad := testNAD()
			nad.Finalizers = nil
			nad, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), nad, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, nad)
			Expect(err).ToNot(HaveOccurred())
			Expect(udn.Finalizers).To(BeEmpty())
		})
		It("when udn is being deleted, and NAD not exist, should remove finalizer from UDN", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			udn := testsUDNWithDeletionTimestamp(time.Now())
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = c.SyncUserDefinedNetwork(udn, nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(udn.Finalizers).To(BeEmpty())
		})
		It("when udn is being deleted, should fail removing finalizer from UDN when patch fails", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			udn := testsUDNWithDeletionTimestamp(time.Now())
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			nad := testNAD()
			nad.Finalizers = nil
			nad, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), nad, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			expectedErr := errors.New("patch UDN error")
			udnClient.PrependReactor("patch", "userdefinednetworks", func(action testing.Action) (bool, runtime.Object, error) {
				return true, nil, expectedErr
			})

			_, err = c.SyncUserDefinedNetwork(udn, nad)
			Expect(err).To(MatchError(expectedErr))
		})

		It("when UDN is being deleted, NAD exist, pod exist, should remove when network not being used", func() {
			udn := testsUDNWithDeletionTimestamp(time.Now())
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			nad := testNAD()
			nad, err = nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), nad, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod1", Namespace: udn.Namespace,
					Annotations: map[string]string{util.OvnPodAnnotationName: `{ 
                          "default": {"role":"primary", "mac_address":"0a:58:0a:f4:02:03"},
						  "test/another-network": {"role": "secondary","mac_address":"0a:58:0a:f4:02:01"} 
                         }`,
					},
				},
			}
			Expect(podInformer.Informer().GetIndexer().Add(pod)).Should(Succeed())
			c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(nad), podInformer)

			nad, err = c.SyncUserDefinedNetwork(udn, nad)
			Expect(err).ToNot(HaveOccurred())

			Expect(nad.Finalizers).To(BeEmpty())
			Expect(udn.Finalizers).To(BeEmpty())
		})

		DescribeTable("when UDN is being deleted, NAD exist, should not remove finalizers when",
			func(testAnnotByPodName map[string]string, expectedErr error) {
				udn := testsUDNWithDeletionTimestamp(time.Now())
				Expect(udnInformer.Informer().GetIndexer().Add(udn)).To(Succeed())

				nad := testNAD()
				Expect(nadInformer.Informer().GetIndexer().Add(nad)).To(Succeed())

				for podName, ovnAnnotValue := range testAnnotByPodName {
					pod := &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name: podName, Namespace: udn.Namespace,
							Annotations: map[string]string{util.OvnPodAnnotationName: ovnAnnotValue},
						},
					}
					Expect(podInformer.Informer().GetIndexer().Add(pod)).Should(Succeed())
				}

				c := New(nadClient, nadInformer, udnClient, udnInformer, renderNadStub(nad), podInformer)

				_, err := c.SyncUserDefinedNetwork(udn, nad)
				Expect(err).To(MatchError(ContainSubstring(expectedErr.Error())))

				actual, _, err := nadInformer.Informer().GetIndexer().Get(nad)
				Expect(err).NotTo(HaveOccurred())
				Expect(actual.(*netv1.NetworkAttachmentDefinition).Finalizers).To(Equal([]string{"k8s.ovn.org/user-defined-network-protection"}),
					"finalizer should remain until no pod uses the network")

				actualUDN, _, err := udnInformer.Informer().GetIndexer().Get(udn)
				Expect(actualUDN.(*udnv1.UserDefinedNetwork).Finalizers).To(Equal([]string{"k8s.ovn.org/user-defined-network-protection"}),
					"finalizer should remain until no pod uses the network")
				Expect(err).NotTo(HaveOccurred())
			},
			Entry("pod connected to user-defined-network primary network",
				map[string]string{
					"test-pod": `{"default":{"role":"infrastructure-locked", "mac_address":"0a:58:0a:f4:02:03"},` +
						`"test/test":{"role": "primary","mac_address":"0a:58:0a:f4:02:01"}}`,
				},
				errors.New("network in use by the following pods: [[test/test-pod]]"),
			),
			Entry("pod connected to default primary network, and user-defined-network secondary network",
				map[string]string{
					"test-pod": `{"default":{"role":"primary", "mac_address":"0a:58:0a:f4:02:03"},` +
						`"test/test":{"role": "secondary","mac_address":"0a:58:0a:f4:02:01"}}`,
				},
				errors.New("network in use by the following pods: [[test/test-pod]]"),
			),
			Entry("1 pod connected to network, 1 pod has invalid annotation",
				map[string]string{
					"test-pod": `{"default":{"role":"primary", "mac_address":"0a:58:0a:f4:02:03"},` +
						`"test/test":{"role": "secondary","mac_address":"0a:58:0a:f4:02:01"}}`,
					"test-pod-invalid-ovn-annot": `invalid`,
				},
				errors.New("failed to filter connected pod due to unmarshal pod annotation [test/test-pod-invalid-ovn-annot]"),
			),
		)
	})

	Context("UserDefinedNetwork status update", func() {
		DescribeTable("should update status, when",
			func(nad *netv1.NetworkAttachmentDefinition, syncErr error, expectedStatus *udnv1.UserDefinedNetworkStatus) {
				udn := testUDN()
				udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())

				c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

				Expect(c.UpdateUserDefinedNetworkStatus(udn, nad, syncErr)).To(Succeed(), "should update status successfully")

				assertUserDefinedNetworkStatus(udnClient, udn, expectedStatus)
			},
			Entry("NAD not exist",
				nil,
				nil,
				&udnv1.UserDefinedNetworkStatus{
					Conditions: []metav1.Condition{
						{
							Type:    "NetworkReady",
							Status:  "False",
							Reason:  "NetworkAttachmentDefinitionNotExist",
							Message: "NetworkAttachmentDefinition is not exist",
						},
					},
				},
			),
			Entry("NAD exist",
				testNAD(),
				nil,
				&udnv1.UserDefinedNetworkStatus{
					Conditions: []metav1.Condition{
						{
							Type:    "NetworkReady",
							Status:  "True",
							Reason:  "NetworkAttachmentDefinitionReady",
							Message: "NetworkAttachmentDefinition has been created",
						},
					},
				},
			),
			Entry("NAD is being deleted",
				testNADWithDeletionTimestamp(time.Now()),
				nil,
				&udnv1.UserDefinedNetworkStatus{
					Conditions: []metav1.Condition{
						{
							Type:    "NetworkReady",
							Status:  "False",
							Reason:  "NetworkAttachmentDefinitionDeleted",
							Message: "NetworkAttachmentDefinition is being deleted",
						},
					},
				},
			),
			Entry("sync error occurred",
				testNAD(),
				errors.New("sync error"),
				&udnv1.UserDefinedNetworkStatus{
					Conditions: []metav1.Condition{
						{
							Type:    "NetworkReady",
							Status:  "False",
							Reason:  "SyncError",
							Message: "sync error",
						},
					},
				},
			),
		)

		It("should update status according to sync errors", func() {
			udn := testUDN()
			udn, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Create(context.Background(), udn, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), nil)

			nad := testNAD()
			syncErr := errors.New("sync error")
			Expect(c.UpdateUserDefinedNetworkStatus(udn, nad, syncErr)).To(Succeed(), "should update status successfully")

			expectedStatus := &udnv1.UserDefinedNetworkStatus{
				Conditions: []metav1.Condition{
					{
						Type:    "NetworkReady",
						Status:  "False",
						Reason:  "SyncError",
						Message: syncErr.Error(),
					},
				},
			}
			assertUserDefinedNetworkStatus(udnClient, udn, expectedStatus)

			anotherSyncErr := errors.New("another sync error")
			Expect(c.UpdateUserDefinedNetworkStatus(udn, nad, anotherSyncErr)).To(Succeed(), "should update status successfully")

			expectedUpdatedStatus := &udnv1.UserDefinedNetworkStatus{
				Conditions: []metav1.Condition{
					{
						Type:    "NetworkReady",
						Status:  "False",
						Reason:  "SyncError",
						Message: anotherSyncErr.Error(),
					},
				},
			}
			assertUserDefinedNetworkStatus(udnClient, udn, expectedUpdatedStatus)
		})

		It("should fail when client update status fails", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, noopRenderNadStub(), podInformer)

			expectedError := errors.New("test err")
			udnClient.PrependReactor("patch", "userdefinednetworks/status", func(action testing.Action) (bool, runtime.Object, error) {
				return true, nil, expectedError
			})

			udn := testUDN()
			nad := testNAD()
			Expect(c.UpdateUserDefinedNetworkStatus(udn, nad, nil)).To(MatchError(expectedError))
		})
	})
})

func assertUserDefinedNetworkStatus(udnClient *udnfakeclient.Clientset, udn *udnv1.UserDefinedNetwork, expectedStatus *udnv1.UserDefinedNetworkStatus) {
	actualUDN, err := udnClient.K8sV1().UserDefinedNetworks(udn.Namespace).Get(context.Background(), udn.Name, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())

	// normalize condition timestamps
	for i := range actualUDN.Status.Conditions {
		actualUDN.Status.Conditions[i].LastTransitionTime = metav1.Time{}
	}
	Expect(actualUDN.Status).To(Equal(*expectedStatus))
}

func renderNadStub(nad *netv1.NetworkAttachmentDefinition) RenderNetAttachDefManifest {
	return newRenderNadStub(nad, nil)
}

func noopRenderNadStub() RenderNetAttachDefManifest {
	return newRenderNadStub(nil, nil)
}

func failRenderNadStub(err error) RenderNetAttachDefManifest {
	return newRenderNadStub(nil, err)
}

func newRenderNadStub(nad *netv1.NetworkAttachmentDefinition, err error) RenderNetAttachDefManifest {
	return func(udn *udnv1.UserDefinedNetwork) (*netv1.NetworkAttachmentDefinition, error) {
		return nad, err
	}
}

type nadRendererStub struct {
	err error
	nad *netv1.NetworkAttachmentDefinition
}

func (s nadRendererStub) RenderNetAttachDefManifest(udn *udnv1.UserDefinedNetwork) (*netv1.NetworkAttachmentDefinition, error) {
	return s.nad, s.err
}

func testUDN() *udnv1.UserDefinedNetwork {
	return &udnv1.UserDefinedNetwork{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test",
			Namespace:  "test",
			UID:        "1",
			Finalizers: []string{"k8s.ovn.org/user-defined-network-protection"},
		},
	}
}

func testsUDNWithDeletionTimestamp(ts time.Time) *udnv1.UserDefinedNetwork {
	udn := testUDN()
	deletionTimestamp := metav1.NewTime(ts)
	udn.DeletionTimestamp = &deletionTimestamp
	return udn
}

func testNAD() *netv1.NetworkAttachmentDefinition {
	return &netv1.NetworkAttachmentDefinition{
		TypeMeta: metav1.TypeMeta{
			Kind:       "k8s.cni.cncf.io/v1",
			APIVersion: "network-attachment-definitions",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test",
			Namespace:  "test",
			Labels:     map[string]string{"k8s.ovn.org/user-defined-network": ""},
			Finalizers: []string{"k8s.ovn.org/user-defined-network-protection"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         udnv1.SchemeGroupVersion.String(),
					Kind:               "UserDefinedNetwork",
					Name:               "test",
					UID:                "1",
					BlockOwnerDeletion: pointer.Bool(true),
				},
			},
		},
		Spec: netv1.NetworkAttachmentDefinitionSpec{},
	}
}

func testNADWithDeletionTimestamp(ts time.Time) *netv1.NetworkAttachmentDefinition {
	nad := testNAD()
	nad.DeletionTimestamp = &metav1.Time{Time: ts}
	return nad
}

func primaryNetNAD() *netv1.NetworkAttachmentDefinition {
	return &netv1.NetworkAttachmentDefinition{
		TypeMeta: metav1.TypeMeta{
			Kind:       "k8s.cni.cncf.io/v1",
			APIVersion: "network-attachment-definitions",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "primary-net-1",
			Namespace: "test",
		},
		Spec: netv1.NetworkAttachmentDefinitionSpec{
			Config: `{"type":"ovn-k8s-cni-overlay","role": "primary"}`,
		},
	}
}
