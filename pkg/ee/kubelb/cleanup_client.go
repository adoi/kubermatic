//go:build ee

/*
                  Kubermatic Enterprise Read-Only License
                         Version 1.0 ("KERO-1.0”)
                     Copyright © 2026 Kubermatic GmbH

   1.	You may only view, read and display for studying purposes the source
      code of the software licensed under this license, and, to the extent
      explicitly provided under this license, the binary code.
   2.	Any use of the software which exceeds the foregoing right, including,
      without limitation, its execution, compilation, copying, modification
      and distribution, is expressly prohibited.
   3.	THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND,
      EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
      MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
      IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
      CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
      TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
      SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

   END OF TERMS AND CONDITIONS
*/

package kubelbcontroller

import (
	"context"
	"fmt"

	kubermaticv1 "k8c.io/kubermatic/sdk/v2/apis/kubermatic/v1"
	kubelbresources "k8c.io/kubermatic/v2/pkg/ee/kubelb/resources"
	"k8c.io/kubermatic/v2/pkg/resources"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// getTenantCleanupClient uses the existing scoped CCM credential. The KubeLB
// manager chart's kubelb-kkp ClusterRole cannot patch TenantState; requiring it
// here would break otherwise valid installations. No new management RBAC is needed.
func (r *reconciler) getTenantCleanupClient(ctx context.Context, cluster *kubermaticv1.Cluster, managementClient ctrlruntimeclient.Client, managementKubeconfig []byte) (ctrlruntimeclient.Client, error) {
	namespace := fmt.Sprintf(kubelbresources.TenantNamespacePattern, cluster.Name)
	if err := managementClient.Get(ctx, ctrlruntimeclient.ObjectKey{Name: namespace}, &corev1.Namespace{}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	secret := &corev1.Secret{}
	err := r.Get(ctx, ctrlruntimeclient.ObjectKey{Namespace: cluster.Status.NamespaceName, Name: resources.KubeLBManagerKubeconfigSecretName}, secret)
	if err == nil {
		return newKubeLBClient(secret.Data[resources.KubeconfigSecretKey])
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	// Installation may have been interrupted before the seed Secret was copied.
	if err := managementClient.Get(ctx, ctrlruntimeclient.ObjectKey{Namespace: namespace, Name: kubeLBCCMKubeconfigSecretName}, secret); err != nil {
		return nil, fmt.Errorf("failed to get tenant credentials for proxy cleanup: %w", err)
	}
	kubeconfig, err := normalizeTenantKubeconfig(secret.Data[kubeconfigSecretKey], managementKubeconfig)
	if err != nil {
		return nil, err
	}
	return newKubeLBClient(kubeconfig)
}
