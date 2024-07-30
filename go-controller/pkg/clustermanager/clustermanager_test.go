package clustermanager

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"github.com/urfave/cli/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/pointer"

	netv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netv1fakeclientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned/fake"
	udnv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1"
	udnfakeclient "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/userdefinednetwork/v1/apis/clientset/versioned/fake"

	hotypes "github.com/ovn-org/ovn-kubernetes/go-controller/hybrid-overlay/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

const (
	// ovnNodeIDAnnotaton is the node annotation name used to store the node id.
	ovnNodeIDAnnotaton = "k8s.ovn.org/node-id"

	// ovnTransitSwitchPortAddrAnnotation is the node annotation name to store the transit switch port ips.
	ovnTransitSwitchPortAddrAnnotation = "k8s.ovn.org/node-transit-switch-port-ifaddr"
)

var _ = ginkgo.Describe("Cluster Manager", func() {
	var (
		app      *cli.App
		f        *factory.WatchFactory
		stopChan chan struct{}
		wg       *sync.WaitGroup
	)

	const (
		clusterIPNet             string = "10.1.0.0"
		clusterCIDR              string = clusterIPNet + "/16"
		clusterv6CIDR            string = "aef0::/48"
		hybridOverlayClusterCIDR string = "11.1.0.0/16/24"
	)

	ginkgo.BeforeEach(func() {
		// Restore global default values before each testcase
		gomega.Expect(config.PrepareTestConfig()).To(gomega.Succeed())

		app = cli.NewApp()
		app.Name = "test"
		app.Flags = config.Flags
		stopChan = make(chan struct{})
		wg = &sync.WaitGroup{}
	})

	ginkgo.AfterEach(func() {
		close(stopChan)
		if f != nil {
			f.Shutdown()
		}
		wg.Wait()
	})

	ginkgo.Context("Node subnet allocations", func() {
		ginkgo.It("Linux nodes", func() {
			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				c, cancel := context.WithCancel(ctx.Context)
				defer cancel()
				clusterManager, err := NewClusterManager(fakeClient, f, "identity", wg, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(c)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer clusterManager.Stop()

				// Check that cluster manager has set the subnet annotation for each node.
				for _, n := range nodes {
					gomega.Eventually(func() ([]*net.IPNet, error) {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return nil, err
						}

						return util.ParseNodeHostSubnetAnnotation(updatedNode, ovntypes.DefaultNetworkName)
					}, 2).Should(gomega.HaveLen(1))
				}

				// Check that the network id 0 is allocated for the default network
				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						networkId, err := util.ParseNetworkIDAnnotation(updatedNode, "default")
						if err != nil {
							return fmt.Errorf("expected node network id annotation for node %s to have been allocated", n.Name)
						}

						gomega.Expect(networkId).To(gomega.Equal(0))
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}
				return nil
			}

			err := app.Run([]string{
				app.Name,
				"-cluster-subnets=" + clusterCIDR,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("Linux nodes - clear subnet annotations", func() {
			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				c, cancel := context.WithCancel(ctx.Context)
				defer cancel()
				clusterManager, err := NewClusterManager(fakeClient, f, "identity", wg, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(c)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer clusterManager.Stop()

				// Check that cluster manager has set the subnet annotation for each node.
				for _, n := range nodes {
					gomega.Eventually(func() ([]*net.IPNet, error) {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return nil, err
						}

						return util.ParseNodeHostSubnetAnnotation(updatedNode, ovntypes.DefaultNetworkName)
					}, 2).Should(gomega.HaveLen(1))
				}

				// Clear the subnet annotation of nodes and make sure it is re-allocated by cluster manager.
				for _, n := range nodes {
					nodeAnnotator := kube.NewNodeAnnotator(&kube.Kube{kubeFakeClient}, n.Name)
					util.DeleteNodeHostSubnetAnnotation(nodeAnnotator)
					err = nodeAnnotator.Run()
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Check that cluster manager has reset the subnet annotation for each node.
				for _, n := range nodes {
					gomega.Eventually(func() ([]*net.IPNet, error) {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return nil, err
						}

						return util.ParseNodeHostSubnetAnnotation(updatedNode, ovntypes.DefaultNetworkName)
					}, 2).Should(gomega.HaveLen(1))
				}

				return nil
			}

			err := app.Run([]string{
				app.Name,
				"-cluster-subnets=" + clusterCIDR,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("Hybrid and linux nodes", func() {

			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "winnode",
							Labels: map[string]string{v1.LabelOSStable: "windows"},
						},
					}}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				c, cancel := context.WithCancel(ctx.Context)
				defer cancel()
				clusterManager, err := NewClusterManager(fakeClient, f, "identity", wg, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(c)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer clusterManager.Stop()

				// Check that cluster manager has set the subnet annotation for each node.
				for _, n := range nodes {
					if n.Name == "winnode" {
						continue
					}

					gomega.Eventually(func() ([]*net.IPNet, error) {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return nil, err
						}

						return util.ParseNodeHostSubnetAnnotation(updatedNode, ovntypes.DefaultNetworkName)
					}, 2).Should(gomega.HaveLen(1))
				}

				return nil
			}

			err := app.Run([]string{
				app.Name,
				"--no-hostsubnet-nodes=kubernetes.io/os=windows",
				"-cluster-subnets=" + clusterCIDR,
				"-gateway-mode=shared",
				"-enable-hybrid-overlay",
				"-hybrid-overlay-cluster-subnets=" + hybridOverlayClusterCIDR,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("Hybrid nodes - clear subnet annotations", func() {

			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "winnode1",
							Labels: map[string]string{v1.LabelOSStable: "windows"},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "winnode2",
							Labels: map[string]string{v1.LabelOSStable: "windows"},
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				c, cancel := context.WithCancel(ctx.Context)
				defer cancel()
				clusterManager, err := NewClusterManager(fakeClient, f, "identity", wg, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(c)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer clusterManager.Stop()

				// Check that cluster manager has set the subnet annotation for each node.
				for _, n := range nodes {
					gomega.Eventually(func() (map[string]string, error) {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return nil, err
						}
						return updatedNode.Annotations, nil
					}, 2).Should(gomega.HaveKey(hotypes.HybridOverlayNodeSubnet))

					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}
						_, err = util.ParseNodeHostSubnetAnnotation(updatedNode, ovntypes.DefaultNetworkName)
						return err
					}, 2).Should(gomega.MatchError("could not find \"k8s.ovn.org/node-subnets\" annotation"))
				}

				// Clear the subnet annotation of nodes and make sure it is re-allocated by cluster manager.
				for _, n := range nodes {
					nodeAnnotator := kube.NewNodeAnnotator(&kube.Kube{kubeFakeClient}, n.Name)

					nodeAnnotations := n.Annotations
					for k, v := range nodeAnnotations {
						nodeAnnotator.Set(k, v)
					}
					nodeAnnotator.Delete(hotypes.HybridOverlayNodeSubnet)
					err = nodeAnnotator.Run()
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				for _, n := range nodes {
					gomega.Eventually(func() (map[string]string, error) {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return nil, err
						}
						return updatedNode.Annotations, nil
					}, 2).Should(gomega.HaveKey(hotypes.HybridOverlayNodeSubnet))

					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}
						_, err = util.ParseNodeHostSubnetAnnotation(updatedNode, ovntypes.DefaultNetworkName)
						return err
					}, 2).Should(gomega.MatchError("could not find \"k8s.ovn.org/node-subnets\" annotation"))
				}
				return nil
			}

			err := app.Run([]string{
				app.Name,
				"--no-hostsubnet-nodes=kubernetes.io/os=windows",
				"-cluster-subnets=" + clusterCIDR,
				"-gateway-mode=shared",
				"-enable-hybrid-overlay",
				"-hybrid-overlay-cluster-subnets=" + hybridOverlayClusterCIDR,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})
	})

	ginkgo.Context("Node Id allocations", func() {
		ginkgo.It("check for node id allocations", func() {
			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				clusterManager, err := NewClusterManager(fakeClient, f, "identity", wg, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer clusterManager.Stop()

				// Check that cluster manager has allocated id for each node
				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						nodeId, ok := updatedNode.Annotations[ovnNodeIDAnnotaton]
						if !ok {
							return fmt.Errorf("expected node annotation for node %s to have node id allocated", n.Name)
						}

						_, err = strconv.Atoi(nodeId)
						if err != nil {
							return fmt.Errorf("expected node annotation for node %s to be an integer value, got %s", n.Name, nodeId)
						}
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}

				return nil
			}

			err := app.Run([]string{
				app.Name,
				"-cluster-subnets=" + clusterCIDR,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("clear the node ids and check", func() {
			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				clusterManager, err := NewClusterManager(fakeClient, f, "identity", wg, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer clusterManager.Stop()

				nodeIds := make(map[string]string)
				// Check that cluster manager has allocated id for each node before clearing
				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						nodeId, ok := updatedNode.Annotations[ovnNodeIDAnnotaton]
						if !ok {
							return fmt.Errorf("expected node annotation for node %s to have node id allocated", n.Name)
						}

						_, err = strconv.Atoi(nodeId)
						if err != nil {
							return fmt.Errorf("expected node annotation for node %s to be an integer value, got %s", n.Name, nodeId)
						}

						nodeIds[n.Name] = nodeId
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}

				// Clear the node id annotation of nodes and make sure it is reset by cluster manager
				// with the same ids.
				for _, n := range nodes {
					nodeAnnotator := kube.NewNodeAnnotator(&kube.Kube{kubeFakeClient}, n.Name)

					nodeAnnotations := n.Annotations
					for k, v := range nodeAnnotations {
						nodeAnnotator.Set(k, v)
					}
					nodeAnnotator.Delete(ovnNodeIDAnnotaton)
					err = nodeAnnotator.Run()
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						nodeId, ok := updatedNode.Annotations[ovnNodeIDAnnotaton]
						if !ok {
							return fmt.Errorf("expected node annotation for node %s to have node id allocated", n.Name)
						}

						_, err = strconv.Atoi(nodeId)
						if err != nil {
							return fmt.Errorf("expected node annotation for node %s to be an integer value, got %s", n.Name, nodeId)
						}

						gomega.Expect(nodeId).To(gomega.Equal(nodeIds[n.Name]))
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}

				return nil
			}

			err := app.Run([]string{
				app.Name,
				"-cluster-subnets=" + clusterCIDR,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("Stop and start a new cluster manager and verify the node ids", func() {
			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				wg1 := &sync.WaitGroup{}
				clusterManager, err := NewClusterManager(fakeClient, f, "cm1", wg1, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Check that cluster manager has allocated id for each node before clearing
				nodeIds := make(map[string]string)
				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						nodeId, ok := updatedNode.Annotations[ovnNodeIDAnnotaton]
						if !ok {
							return fmt.Errorf("expected node annotation for node %s to have node id allocated", n.Name)
						}

						_, err = strconv.Atoi(nodeId)
						if err != nil {
							return fmt.Errorf("expected node annotation for node %s to be an integer value, got %s", n.Name, nodeId)
						}

						nodeIds[n.Name] = nodeId
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}

				updatedNodes := []v1.Node{}
				for _, n := range nodes {
					updatedNode, _ := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
					updatedNodes = append(updatedNodes, *updatedNode)
				}
				// stop the cluster manager and start a new instance and make sure the node ids are same.
				clusterManager.Stop()
				wg1.Wait()

				// Close the watch factory and create a new one
				f.Shutdown()
				kubeFakeClient = fake.NewSimpleClientset(&v1.NodeList{
					Items: updatedNodes,
				})
				fakeClient = &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}
				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				cm2, err := NewClusterManager(fakeClient, f, "cm2", wg, nil)
				gomega.Expect(cm2).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = cm2.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer cm2.Stop()

				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						nodeId, ok := updatedNode.Annotations[ovnNodeIDAnnotaton]
						if !ok {
							return fmt.Errorf("expected node annotation for node %s to have node id allocated", n.Name)
						}

						_, err = strconv.Atoi(nodeId)
						if err != nil {
							return fmt.Errorf("expected node annotation for node %s to be an integer value, got %s", n.Name, nodeId)
						}

						gomega.Expect(nodeId).To(gomega.Equal(nodeIds[n.Name]))
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}

				return nil
			}

			err := app.Run([]string{
				app.Name,
				"-cluster-subnets=" + clusterCIDR,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("Stop cluster manager, set duplicate id, restart and verify the node ids", func() {
			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				wg1 := &sync.WaitGroup{}
				clusterManager, err := NewClusterManager(fakeClient, f, "cm1", wg1, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				nodeIds := make(map[string]string)
				// Check that cluster manager has allocated id for each node before clearing
				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						nodeId, ok := updatedNode.Annotations[ovnNodeIDAnnotaton]
						if !ok {
							return fmt.Errorf("expected node annotation for node %s to have node id allocated", n.Name)
						}

						_, err = strconv.Atoi(nodeId)
						if err != nil {
							return fmt.Errorf("expected node annotation for node %s to be an integer value, got %s", n.Name, nodeId)
						}

						nodeIds[n.Name] = nodeId
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}

				// stop the cluster manager.
				clusterManager.Stop()
				wg1.Wait()

				updatedNodes := []v1.Node{}
				node2, _ := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node2", metav1.GetOptions{})
				for _, n := range nodes {
					updatedNode, _ := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
					if updatedNode.Name == "node3" {
						// Make the id of node3 duplicate.
						updatedNode.Annotations[ovnNodeIDAnnotaton] = node2.Annotations[ovnNodeIDAnnotaton]
					}
					updatedNodes = append(updatedNodes, *updatedNode)
				}

				// Close the watch factory and create a new one
				f.Shutdown()
				kubeFakeClient = fake.NewSimpleClientset(&v1.NodeList{
					Items: updatedNodes,
				})
				fakeClient = &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}
				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Start a new cluster manager
				cm2, err := NewClusterManager(fakeClient, f, "cm2", wg, nil)
				gomega.Expect(cm2).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = cm2.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer cm2.Stop()

				// Get the node ids of node2 and node3 and make sure that they are not equal
				gomega.Eventually(func() error {
					n2, _ := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node2", metav1.GetOptions{})
					n3, _ := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node3", metav1.GetOptions{})
					n2Id := n2.Annotations[ovnNodeIDAnnotaton]
					n3Id := n3.Annotations[ovnNodeIDAnnotaton]
					if n2Id == n3Id {
						return fmt.Errorf("expected node annotation for node2 and node3 to be not equal, but they are : node id %s", n2Id)
					}
					return nil
				}).ShouldNot(gomega.HaveOccurred())

				return nil
			}

			err := app.Run([]string{
				app.Name,
				"-cluster-subnets=" + clusterCIDR,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})
	})

	ginkgo.Context("Node gateway router port IP allocations", func() {
		ginkgo.It("verify the node annotations", func() {
			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				clusterManager, err := NewClusterManager(fakeClient, f, "identity", wg, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer clusterManager.Stop()

				// Check that cluster manager has set the node-gateway-router-lrp-ifaddr annotation for each node.
				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						gwLRPAddrs, err := util.ParseNodeGatewayRouterJoinAddrs(updatedNode, types.DefaultNetworkName)
						if err != nil {
							return err
						}

						gomega.Expect(gwLRPAddrs).NotTo(gomega.BeNil())
						gomega.Expect(len(gwLRPAddrs)).To(gomega.Equal(2))
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}

				return nil
			}

			err := app.Run([]string{
				app.Name,
				"-cluster-subnets=" + clusterCIDR + "," + clusterv6CIDR,
				"-k8s-service-cidr=10.96.0.0/16,fd00:10:96::/112",
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("clear the node annotations for gateway router port ips and check", func() {
			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				clusterManager, err := NewClusterManager(fakeClient, f, "identity", wg, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer clusterManager.Stop()

				nodeAddrs := make(map[string]string)
				// Check that cluster manager has set the node-gateway-router-lrp-ifaddr annotation for each node.
				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						gwLRPAddrs, err := util.ParseNodeGatewayRouterJoinAddrs(updatedNode, types.DefaultNetworkName)
						if err != nil {
							return err
						}
						gomega.Expect(gwLRPAddrs).NotTo(gomega.BeNil())
						gomega.Expect(len(gwLRPAddrs)).To(gomega.Equal(2))
						nodeAddrs[n.Name] = updatedNode.Annotations[util.OVNNodeGRLRPAddrs]
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}

				// Clear the node-gateway-router-lrp-ifaddr annotation of nodes and make sure it is reset by cluster manager
				// with the same addrs.
				for _, n := range nodes {
					nodeAnnotator := kube.NewNodeAnnotator(&kube.Kube{kubeFakeClient}, n.Name)

					nodeAnnotations := n.Annotations
					for k, v := range nodeAnnotations {
						nodeAnnotator.Set(k, v)
					}
					nodeAnnotator.Delete(util.OVNNodeGRLRPAddrs)
					err = nodeAnnotator.Run()
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						nodeGWRPIPs, ok := updatedNode.Annotations[util.OVNNodeGRLRPAddrs]
						if !ok {
							return fmt.Errorf("expected node annotation for node %s to have node gateway-router-lrp-ifaddr allocated", n.Name)
						}

						gomega.Expect(nodeGWRPIPs).To(gomega.Equal(nodeAddrs[n.Name]))
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}
				return nil
			}

			err := app.Run([]string{
				app.Name,
				"-cluster-subnets=" + clusterCIDR + "," + clusterv6CIDR,
				"-k8s-service-cidr=10.96.0.0/16,fd00:10:96::/112",
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		ginkgo.It("Stop cluster manager, change id of a node and verify the gateway router port addr node annotation", func() {
			app.Action = func(ctx *cli.Context) error {
				nodes := []v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node2",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node3",
						},
					},
				}
				kubeFakeClient := fake.NewSimpleClientset(&v1.NodeList{
					Items: nodes,
				})
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				config.Kubernetes.HostNetworkNamespace = ""

				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				wg1 := &sync.WaitGroup{}
				clusterManager, err := NewClusterManager(fakeClient, f, "identity", wg1, nil)
				gomega.Expect(clusterManager).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = clusterManager.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				node3GWRPAnnotation := ""
				// Check that cluster manager has set the node-gateway-router-lrp-ifaddr annotation for each node.
				for _, n := range nodes {
					gomega.Eventually(func() error {
						updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
						if err != nil {
							return err
						}

						gwLRPAddrs, err := util.ParseNodeGatewayRouterJoinAddrs(updatedNode, types.DefaultNetworkName)
						if err != nil {
							return err
						}
						gomega.Expect(gwLRPAddrs).NotTo(gomega.BeNil())
						gomega.Expect(len(gwLRPAddrs)).To(gomega.Equal(2))

						// Store the node 3's gw router port addresses
						if updatedNode.Name == "node3" {
							node3GWRPAnnotation = updatedNode.Annotations[util.OVNNodeGRLRPAddrs]
						}
						return nil
					}).ShouldNot(gomega.HaveOccurred())
				}

				// stop the cluster manager.
				clusterManager.Stop()
				wg1.Wait()

				updatedNodes := []v1.Node{}

				for _, n := range nodes {
					updatedNode, _ := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), n.Name, metav1.GetOptions{})
					if updatedNode.Name == "node3" {
						// Change the id of node3 duplicate.
						updatedNode.Annotations[ovnNodeIDAnnotaton] = "50"
					}
					updatedNodes = append(updatedNodes, *updatedNode)
				}

				// Close the watch factory and create a new one
				f.Shutdown()
				kubeFakeClient = fake.NewSimpleClientset(&v1.NodeList{
					Items: updatedNodes,
				})
				fakeClient = &util.OVNClusterManagerClientset{
					KubeClient: kubeFakeClient,
				}
				f, err = factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = f.Start()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Start a new cluster manager
				cm2, err := NewClusterManager(fakeClient, f, "cm2", wg, nil)
				gomega.Expect(cm2).NotTo(gomega.BeNil())
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = cm2.Start(ctx.Context)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer cm2.Stop()

				gomega.Eventually(func() error {
					updatedNode, err := fakeClient.KubeClient.CoreV1().Nodes().Get(context.TODO(), "node3", metav1.GetOptions{})
					if err != nil {
						return err
					}

					node3UpdatedGWRPAnnotation := updatedNode.Annotations[util.OVNNodeGRLRPAddrs]
					gomega.Expect(node3UpdatedGWRPAnnotation).NotTo(gomega.Equal(node3GWRPAnnotation))

					gwLRPAddrs, err := util.ParseNodeGatewayRouterJoinAddrs(updatedNode, types.DefaultNetworkName)
					if err != nil {
						return err
					}
					gomega.Expect(gwLRPAddrs).NotTo(gomega.BeNil())
					gomega.Expect(len(gwLRPAddrs)).To(gomega.Equal(2))
					return nil
				}).ShouldNot(gomega.HaveOccurred())
				return nil
			}

			err := app.Run([]string{
				app.Name,
				"-cluster-subnets=" + clusterCIDR + "," + clusterv6CIDR,
				"-k8s-service-cidr=10.96.0.0/16,fd00:10:96::/112",
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})
	})

	ginkgo.Context("user-defined-network controller", func() {
		var (
			nadClient *netv1fakeclientset.Clientset
			udnClient *udnfakeclient.Clientset

			cm *ClusterManager
		)

		ginkgo.BeforeEach(func() {
			app.Action = func(ctx *cli.Context) error {
				config.OVNKubernetesFeature.EnableMultiNetwork = true
				config.OVNKubernetesFeature.EnableNetworkSegmentation = true

				kubeFakeClient := fake.NewSimpleClientset()
				nadClient = netv1fakeclientset.NewSimpleClientset()
				udnClient = udnfakeclient.NewSimpleClientset()
				fakeClient := &util.OVNClusterManagerClientset{
					KubeClient:               kubeFakeClient,
					NetworkAttchDefClient:    nadClient,
					UserDefinedNetworkClient: udnClient,
				}

				_, err := config.InitConfig(ctx, nil, nil)
				if err != nil {
					return err
				}
				config.Kubernetes.HostNetworkNamespace = ""

				f, err := factory.NewClusterManagerWatchFactory(fakeClient)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(f.Start()).To(gomega.Succeed())

				cm, err = NewClusterManager(fakeClient, f, "identity", wg, nil)
				gomega.Expect(cm.Start(ctx.Context)).To(gomega.Succeed())
				return nil
			}
			err := app.Run([]string{
				app.Name,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})
		ginkgo.AfterEach(func() {
			cm.Stop()
		})

		ginkgo.FIt("should create NAD according to UDN spec", func() {
			testNamespace := "test"

			udn, err := udnClient.K8sV1().UserDefinedNetworks(testNamespace).Create(context.Background(), testUDN(), metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Eventually(func() []metav1.Condition {
				updatedUDN, err := udnClient.K8sV1().UserDefinedNetworks(testNamespace).Get(context.Background(), udn.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				return normalizeConditions(updatedUDN.Status.Conditions)
			}).Should(gomega.Equal([]metav1.Condition{{
				Type:    "NetworkReady",
				Status:  "True",
				Reason:  "NetworkAttachmentDefinitionReady",
				Message: "NetworkAttachmentDefinition has been created",
			}}))

			nad, err := nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(testNamespace).Get(context.Background(), udn.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(nad).To(gomega.Equal(testNAD()))
		})
	})

})

func testUDN() *udnv1.UserDefinedNetwork {
	return &udnv1.UserDefinedNetwork{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test", Namespace: "test", UID: "1",
		},
		Spec: udnv1.UserDefinedNetworkSpec{
			Role: "Secondary", Topology: "Layer2",
		},
	}
}

func testNAD() *netv1.NetworkAttachmentDefinition {
	return &netv1.NetworkAttachmentDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "k8s.cni.cncf.io/v1",
			Kind:       "NetworkAttachmentDefinition",
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
					Controller:         pointer.Bool(true),
				},
			},
		},
		Spec: netv1.NetworkAttachmentDefinitionSpec{
			Config: "{\"cniVersion\":\"1.0.0\",\"name\":\"test.test\",\"netAttachDefName\":\"test/test\",\"role\":\"secondary\",\"topology\":\"layer2\",\"type\":\"ovn-k8s-cni-overlay\"}",
		},
	}
}

func normalizeConditions(conditions []metav1.Condition) []metav1.Condition {
	for i := range conditions {
		t := metav1.NewTime(time.Time{})
		conditions[i].LastTransitionTime = t
	}
	return conditions
}
