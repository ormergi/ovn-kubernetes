package cluster_context

import (
	"k8s.io/client-go/kubernetes"
	e2eframework "k8s.io/kubernetes/test/e2e/framework"
)

// Exec run the given function in context of the given cluster.
// using its client (kubeClient), kubeconfig (kubeConf) and API URL (kubeHost).
// It overrides the test framework globals and revert back to original values.
func Exec(
	f *e2eframework.Framework,
	kubeClient kubernetes.Interface,
	kubeConf string,
	kubeHost string,
	fn func() error,
) error {
	originalFrameworkClient := f.ClientSet
	originalKubeConf := e2eframework.TestContext.KubeConfig
	originalKubeHost := e2eframework.TestContext.Host
	f.ClientSet = kubeClient
	e2eframework.TestContext.KubeConfig = kubeConf
	e2eframework.TestContext.Host = kubeHost
	err := fn()
	f.ClientSet = originalFrameworkClient
	e2eframework.TestContext.KubeConfig = originalKubeConf
	e2eframework.TestContext.Host = originalKubeHost
	return err
}

func ExecOutput(
	f *e2eframework.Framework,
	kubeClient kubernetes.Interface,
	kubeConf string,
	kubeHost string,
	fn func() (string, error),
) (string, error){
	originalFrameworkClient := f.ClientSet
	originalKubeConf := e2eframework.TestContext.KubeConfig
	originalKubeHost := e2eframework.TestContext.Host
	f.ClientSet = kubeClient
	e2eframework.TestContext.KubeConfig = kubeConf
	e2eframework.TestContext.Host = kubeHost
	output, err := fn()
	f.ClientSet = originalFrameworkClient
	e2eframework.TestContext.KubeConfig = originalKubeConf
	e2eframework.TestContext.Host = originalKubeHost
	return output, err
}
