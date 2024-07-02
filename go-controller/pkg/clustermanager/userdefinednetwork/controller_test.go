package userdefinednetwork

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	netv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netv1fakeclientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned/fake"
	netv1informerfactory "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions"
	netv1Informer "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions/k8s.cni.cncf.io/v1"

	udnv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1"
	udnfakeclient "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/clientset/versioned/fake"
	udninformerfactory "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/informers/externalversions"
	udninformer "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/informers/externalversions/userdefinednetwork/v1"
)

var _ = Describe("User Defined Network Controller", func() {
	var (
		udnClient   *udnfakeclient.Clientset
		nadClient   *netv1fakeclientset.Clientset
		udnInformer udninformer.UserDefinedNetworkInformer
		nadInformer netv1Informer.NetworkAttachmentDefinitionInformer
	)

	BeforeEach(func() {
		udnClient = udnfakeclient.NewSimpleClientset()
		udnInformer = udninformerfactory.NewSharedInformerFactory(udnClient, 15).K8s().V1().UserDefinedNetworks()
		nadClient = netv1fakeclientset.NewSimpleClientset()
		nadInformer = netv1informerfactory.NewSharedInformerFactory(nadClient, 15).K8sCniCncfIo().V1().NetworkAttachmentDefinitions()
	})

	Context("reconciler", func() {
		It("should fail", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, nadRendererStub{})

			Expect(c.UserDefinedNetworkReconciler("test/test")).ToNot(Succeed())
		})
		It("should fail when parsing key fails", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, nadRendererStub{})

			Expect(c.UserDefinedNetworkReconciler("a//a")).ToNot(Succeed())
		})
	})

	Context("UserDefinedNetwork object sync", func() {
		It("should fail", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, nadRendererStub{})
			err := c.SyncUserDefinedNetwork(nil, nil)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("UserDefinedNetwork status update", func() {
		It("should fail", func() {
			c := New(nadClient, nadInformer, udnClient, udnInformer, nadRendererStub{})
			Expect(c.UpdateUserDefinedNetworkStatus(nil, nil, nil)).ToNot(Succeed())
		})
	})
})

type nadRendererStub struct {
	err error
	nad *netv1.NetworkAttachmentDefinition
}

func (s nadRendererStub) RenderNetAttachDefManifest(udn *udnv1.UserDefinedNetwork) (*netv1.NetworkAttachmentDefinition, error) {
	return s.nad, s.err
}
