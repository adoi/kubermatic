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
	"path"
	"strings"

	kubermaticv1 "k8c.io/kubermatic/sdk/v2/apis/kubermatic/v1"
	kubelbresources "k8c.io/kubermatic/v2/pkg/ee/kubelb/resources"
	kuberneteshelper "k8c.io/kubermatic/v2/pkg/kubernetes"
	"k8c.io/kubermatic/v2/pkg/resources"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// These names are the CCM's lifecycle contract in KubeLB EE v1.5.0.
	// Keep the optional APIs unstructured; KKP also supports older CCMs.
	kubeLBSourceFinalizer        = "kubelb.k8c.io/cleanup"
	kubeLBLegacyServiceFinalizer = "kubelb.k8c.io/lb-finalizer"
	tenantProxyFinalizer         = "kubelb.k8c.io/tenant-proxy-finalizer"
	tenantProxyName              = "kubelb-tenant-envoy"
	tenantProxyRoleName          = "kubelb-tenant-proxy-xds-writer"
)

func drainTenantWAFPolicies(ctx context.Context, client ctrlruntimeclient.Client) (bool, error) {
	policies := &unstructured.UnstructuredList{}
	policies.SetAPIVersion("kubelb.k8c.io/v1alpha1")
	policies.SetKind("TenantWAFPolicyList")
	if err := client.List(ctx, policies); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to list TenantWAFPolicies: %w", err)
	}
	objects := make([]ctrlruntimeclient.Object, 0, len(policies.Items))
	for i := range policies.Items {
		objects = append(objects, &policies.Items[i])
	}
	return deleteCleanupResources(ctx, client, objects)
}

func (r *reconciler) cleanupTenantProxy(ctx context.Context, cluster *kubermaticv1.Cluster, userClient ctrlruntimeclient.Client, getTenantClient tenantCleanupClientGetter) (bool, error) {
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, ctrlruntimeclient.ObjectKey{Namespace: cluster.Status.NamespaceName, Name: resources.KubeLBDeploymentName}, deployment); err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	namespace, customServiceAccount, err := tenantProxySettings(deployment)
	if err != nil {
		return false, err
	}
	workloads := []ctrlruntimeclient.Object{
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName, Namespace: namespace}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName, Namespace: namespace}},
	}
	if done, err := deleteCleanupResources(ctx, userClient, workloads); err != nil || !done {
		return done, err
	}
	selector := ctrlruntimeclient.MatchingLabels{
		"app.kubernetes.io/name": tenantProxyName,
		"kubelb.k8c.io/tenant":   fmt.Sprintf(kubelbresources.TenantNamespacePattern, cluster.Name),
	}
	// A Deployment deleted with orphan propagation leaves ReplicaSets running.
	// Remove these first so they cannot recreate Pods during cleanup.
	replicaSets := &appsv1.ReplicaSetList{}
	if err := userClient.List(ctx, replicaSets, ctrlruntimeclient.InNamespace(namespace), selector); err != nil {
		return false, err
	}
	orphans := make([]ctrlruntimeclient.Object, 0, len(replicaSets.Items))
	for i := range replicaSets.Items {
		orphans = append(orphans, &replicaSets.Items[i])
	}
	if done, err := deleteOrphanedTenantProxyResources(ctx, userClient, orphans); err != nil || !done {
		return done, err
	}
	// Keep configuration, credentials and RBAC while proxy Pods drain. Surviving
	// orphans need an explicit delete; no workload controller will remove them.
	pods := &corev1.PodList{}
	if err := userClient.List(ctx, pods, ctrlruntimeclient.InNamespace(namespace), selector); err != nil {
		return false, err
	}
	orphans = make([]ctrlruntimeclient.Object, 0, len(pods.Items))
	for i := range pods.Items {
		orphans = append(orphans, &pods.Items[i])
	}
	if done, err := deleteOrphanedTenantProxyResources(ctx, userClient, orphans); err != nil || !done {
		return done, err
	}
	objects := []ctrlruntimeclient.Object{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName, Namespace: namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kubelb-tenant-proxy-config", Namespace: namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "kubelb-tenant-proxy-server-tls", Namespace: namespace}},
		&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "kubelb-tenant-proxy-ingress", Namespace: namespace}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyRoleName}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyRoleName}},
	}
	// KubeLB deliberately retains externally provided ServiceAccounts.
	if !customServiceAccount {
		objects = append(objects, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName, Namespace: namespace}})
	}
	if done, err := deleteCleanupResources(ctx, userClient, objects); err != nil || !done {
		return done, err
	}

	tenantClient, err := getTenantClient(ctx)
	if err != nil {
		return false, err
	}
	if tenantClient == nil {
		// The tenant namespace was never created or has already disappeared.
		return true, nil
	}
	state := kubeLBObject("TenantState", fmt.Sprintf(kubelbresources.TenantNamespacePattern, cluster.Name), "default")
	if err := tenantClient.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(state), state); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to get TenantState for proxy cleanup: %w", err)
	}
	// KKP has completed the exact cleanup guarded by this CCM finalizer. Never
	// remove any other finalizers. Do this BEFORE deleting the management Tenant,
	// whose controller revokes the tenant credentials during finalization.
	if err := removeKubeLBFinalizers(ctx, tenantClient, state, tenantProxyFinalizer); err != nil {
		return false, err
	}
	return true, nil
}

// Read the deployed settings, rather than current cluster/DC defaults, which may
// have changed since the CCM was installed. Its Deployment survives until the
// final cleanup phase, including when reconciliation resumes after a restart.
func tenantProxySettings(deployment *appsv1.Deployment) (string, bool, error) {
	namespace := metav1.NamespaceSystem
	customServiceAccount := false
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != resources.KubeLBDeploymentName {
			continue
		}
		args := container.Args
		if len(container.Command) > 0 && path.Base(container.Command[0]) == "http-prober" {
			found, command := resources.UnwrapCommand(container)
			if !found || command.Command == "" {
				return "", false, fmt.Errorf("cannot read tenant proxy settings: invalid CCM http-prober command")
			}
			args = command.Args
		} else if len(container.Command) > 1 {
			args = append(append([]string{}, container.Command[1:]...), args...)
		}
		for i, arg := range args {
			if !strings.HasPrefix(arg, "-") {
				continue
			}
			key, value, found := strings.Cut(strings.TrimLeft(arg, "-"), "=")
			if !found && i+1 < len(args) {
				value = args[i+1]
			}
			switch key {
			case "leader-election-namespace":
				namespace = value
			case "tenant-proxy-service-account-name":
				customServiceAccount = value != ""
			}
		}
		for _, env := range container.Env {
			if env.Name == "NAMESPACE" && env.Value != "" {
				namespace = env.Value
			}
		}
	}
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}
	return namespace, customServiceAccount, nil
}

// Call only with objects selected by both proxy and tenant labels. Keep live
// owners and unfamiliar controllers intact; only delete objects whose expected
// proxy controller has disappeared (or whose controller reference was removed).
func deleteOrphanedTenantProxyResources(ctx context.Context, client ctrlruntimeclient.Client, objects []ctrlruntimeclient.Object) (bool, error) {
	for _, object := range objects {
		if object.GetDeletionTimestamp() != nil {
			continue
		}
		if owner := metav1.GetControllerOf(object); owner != nil {
			var controller ctrlruntimeclient.Object
			if owner.APIVersion == appsv1.SchemeGroupVersion.String() {
				switch object.(type) {
				case *appsv1.ReplicaSet:
					if owner.Kind == "Deployment" && owner.Name == tenantProxyName {
						controller = &appsv1.Deployment{}
					}
				case *corev1.Pod:
					switch {
					case owner.Kind == "DaemonSet" && owner.Name == tenantProxyName:
						controller = &appsv1.DaemonSet{}
					case owner.Kind == "ReplicaSet" && strings.HasPrefix(owner.Name, tenantProxyName+"-"):
						controller = &appsv1.ReplicaSet{}
					}
				}
			}
			if controller == nil {
				return false, fmt.Errorf("cannot delete proxy resource %s: unexpected controller %s %s", ctrlruntimeclient.ObjectKeyFromObject(object), owner.Kind, owner.Name)
			}
			err := client.Get(ctx, ctrlruntimeclient.ObjectKey{Namespace: object.GetNamespace(), Name: owner.Name}, controller)
			if err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
			if err == nil && controller.GetUID() == owner.UID {
				continue
			}
		}
		// Use the listed object's identity AND version: a replacement, relabeling
		// or concurrent controller adoption must invalidate this deletion.
		uid, version := object.GetUID(), object.GetResourceVersion()
		err := client.Delete(ctx, object, ctrlruntimeclient.PropagationPolicy(metav1.DeletePropagationForeground), &ctrlruntimeclient.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version},
		})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	// Even an accepted deletion must be observed to finish before credentials or
	// the TenantState finalizer can be removed.
	return len(objects) == 0, nil
}

// removeKubeLBSourceFinalizers detaches native user resources, rather than
// deleting them. It runs only after the CCM is stopped and the entire management
// tenant namespace (including every mirror) has disappeared.
func removeKubeLBSourceFinalizers(ctx context.Context, client ctrlruntimeclient.Client) error {
	kinds := []schema.GroupKind{
		{Kind: "Service"},
		{Kind: "Secret"},
		{Group: "networking.k8s.io", Kind: "Ingress"},
		{Group: "kubelb.k8c.io", Kind: "SyncSecret"},
		{Group: "kubelb.k8c.io", Kind: "TenantWAFPolicy"},
		{Group: "gateway.networking.k8s.io", Kind: "Gateway"},
		{Group: "gateway.networking.k8s.io", Kind: "HTTPRoute"},
		{Group: "gateway.networking.k8s.io", Kind: "GRPCRoute"},
		{Group: "gateway.networking.k8s.io", Kind: "TCPRoute"},
		{Group: "gateway.networking.k8s.io", Kind: "UDPRoute"},
		{Group: "gateway.networking.k8s.io", Kind: "TLSRoute"},
		{Group: "gateway.envoyproxy.io", Kind: "BackendTrafficPolicy"},
		{Group: "gateway.envoyproxy.io", Kind: "ClientTrafficPolicy"},
	}
	for _, kind := range kinds {
		mapping, err := client.RESTMapper().RESTMapping(kind)
		if meta.IsNoMatchError(err) {
			continue
		}
		if err != nil {
			return err
		}
		objects := &metav1.PartialObjectMetadataList{}
		objects.SetGroupVersionKind(mapping.GroupVersionKind.GroupVersion().WithKind(kind.Kind + "List"))
		if err := client.List(ctx, objects); err != nil {
			if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
				continue
			}
			return fmt.Errorf("failed to list %s for finalizer cleanup: %w", kind, err)
		}
		for i := range objects.Items {
			object := &objects.Items[i]
			object.SetGroupVersionKind(mapping.GroupVersionKind)
			finalizers := []string{kubeLBSourceFinalizer}
			if kind.Group == "" && kind.Kind == "Service" {
				finalizers = append(finalizers, kubeLBLegacyServiceFinalizer)
			}
			if !kuberneteshelper.HasAnyFinalizer(object, finalizers...) {
				continue
			}
			if err := removeKubeLBFinalizers(ctx, client, object, finalizers...); err != nil {
				return err
			}
		}
	}
	return nil
}

// Use a resourceVersion precondition because finalizers are an atomic list.
// A concurrent controller adding a finalizer must not lose it to our merge patch.
func removeKubeLBFinalizers(ctx context.Context, client ctrlruntimeclient.Client, object ctrlruntimeclient.Object, finalizers ...string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := client.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(object), object); err != nil {
			return ctrlruntimeclient.IgnoreNotFound(err)
		}
		original := object.DeepCopyObject().(ctrlruntimeclient.Object)
		kuberneteshelper.RemoveFinalizer(object, finalizers...)
		if len(original.GetFinalizers()) == len(object.GetFinalizers()) {
			return nil
		}
		return ctrlruntimeclient.IgnoreNotFound(client.Patch(ctx, object, ctrlruntimeclient.MergeFromWithOptions(original, ctrlruntimeclient.MergeFromWithOptimisticLock{})))
	})
}
