//go:build ee

/*
                  Kubermatic Enterprise Read-Only License
                         Version 1.0 ("KERO-1.0”)
                     Copyright © 2021 Kubermatic GmbH

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
	kubelbmanagementresources "k8c.io/kubermatic/v2/pkg/ee/kubelb/resources/kubelb-cluster"
	kubelbseedresources "k8c.io/kubermatic/v2/pkg/ee/kubelb/resources/seed-cluster"
	kubelbuserclusterresources "k8c.io/kubermatic/v2/pkg/ee/kubelb/resources/user-cluster"
	kuberneteshelper "k8c.io/kubermatic/v2/pkg/kubernetes"
	"k8c.io/kubermatic/v2/pkg/resources"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// Persist teardown progress so retries do not need credentials already removed
	// by the Tenant controller, and re-enabling KubeLB cannot restart the CCM midway.
	cleanupPhaseAnnotation = "kubermatic.k8c.io/kubelb-cleanup-phase"
	cleanupDrainWAF        = "drain-waf"
	cleanupStopCCM         = "stop-ccm"
	cleanupProxy           = "remove-proxy"
	cleanupTenant          = "remove-tenant"
	cleanupUserCluster     = "remove-user-cluster-resources"
	cleanupSeedCluster     = "remove-seed-cluster-resources"
)

// cleanupPhaseWaiting describes what each phase blocks on, for operators
// watching a pending cleanup. Keep the messages static so the recorder
// aggregates the periodic events into a single series.
var cleanupPhaseWaiting = map[string]string{
	cleanupDrainWAF:    "all TenantWAFPolicies in the user cluster to be finalized and removed",
	cleanupStopCCM:     "the KubeLB CCM Deployment to scale down and its Pods to terminate",
	cleanupProxy:       "tenant proxy resources in the user cluster to finish terminating",
	cleanupTenant:      "the Tenant and its namespace to be removed from the KubeLB management cluster",
	cleanupUserCluster: "KubeLB RBAC and CRDs in the user cluster to be removed",
	cleanupSeedCluster: "KubeLB resources in the user cluster namespace to be removed",
}

type tenantCleanupClientGetter func(context.Context) (ctrlruntimeclient.Client, error)

// handleKubeLBCleanup surfaces failures as events on the Cluster: unlike
// installation, cleanup runs outside ClusterReconcileWrapper, and a stuck
// cluster deletion would otherwise only be visible in the controller log.
func (r *reconciler) handleKubeLBCleanup(ctx context.Context, cluster *kubermaticv1.Cluster) (reconcile.Result, error) {
	result, err := r.runKubeLBCleanup(ctx, cluster)
	if err != nil {
		r.recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "KubeLBCleanupError", "KubeLBCleanup", err.Error())
	}
	return result, err
}

func (r *reconciler) runKubeLBCleanup(ctx context.Context, cluster *kubermaticv1.Cluster) (reconcile.Result, error) {
	// No clients are needed to record the start or finish seed-side cleanup.
	phase := cluster.Annotations[cleanupPhaseAnnotation]
	if phase == "" || phase == cleanupStopCCM || phase == cleanupSeedCluster {
		return r.reconcileKubeLBCleanup(ctx, cluster, nil, nil, nil)
	}

	userClient, err := r.userClusterConnectionProvider.GetClient(ctx, cluster)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to get user cluster client: %w", err)
	}
	if phase == cleanupDrainWAF || phase == cleanupUserCluster {
		return r.reconcileKubeLBCleanup(ctx, cluster, userClient, nil, nil)
	}

	seed, err := r.seedGetter()
	if err != nil {
		return reconcile.Result{}, err
	}
	dc, found := seed.Spec.Datacenters[cluster.Spec.Cloud.DatacenterName]
	if !found {
		return reconcile.Result{}, fmt.Errorf("couldn't find datacenter %q for cluster %q", cluster.Spec.Cloud.DatacenterName, cluster.Name)
	}
	managementClient, managementKubeconfig, err := r.getKubeLBManagementClusterClient(ctx, seed, dc)
	if err != nil {
		return reconcile.Result{}, err
	}
	getTenantClient := func(ctx context.Context) (ctrlruntimeclient.Client, error) {
		return r.getTenantCleanupClient(ctx, cluster, managementClient, managementKubeconfig)
	}
	return r.reconcileKubeLBCleanup(ctx, cluster, userClient, managementClient, getTenantClient)
}

// reconcileKubeLBCleanup deliberately advances at most one phase per reconcile.
// A successful Delete only requests deletion; every phase waits for its resources
// to disappear before it relinquishes the credentials or finalizers they need.
func (r *reconciler) reconcileKubeLBCleanup(ctx context.Context, cluster *kubermaticv1.Cluster, userClient, managementClient ctrlruntimeclient.Client, getTenantClient tenantCleanupClientGetter) (reconcile.Result, error) {
	phase := cluster.Annotations[cleanupPhaseAnnotation]
	next := cleanupDrainWAF
	done := true
	var err error
	switch phase {
	case "":
	case cleanupDrainWAF:
		// The CCM still runs and retains both kubeconfigs and its RBAC. Let it
		// release WAF policy finalizers while the management Routes still exist.
		done, err = drainTenantWAFPolicies(ctx, userClient)
		next = cleanupStopCCM
	case cleanupStopCCM:
		done, err = r.stopKubeLBCCM(ctx, cluster.Status.NamespaceName)
		next = cleanupProxy
	case cleanupProxy:
		// The v1.5 CCM has no teardown mode. Once it is stopped, KKP fulfills
		// its proxy cleanup obligation instead of racing its normal reconciles.
		done, err = r.cleanupTenantProxy(ctx, cluster, userClient, getTenantClient)
		next = cleanupTenant
	case cleanupTenant:
		done, err = removeKubeLBTenant(ctx, managementClient, cluster.Name)
		next = cleanupUserCluster
	case cleanupUserCluster:
		// Management mirrors are gone and the CCM cannot recreate them. It is
		// now safe to release only KubeLB's finalizers on retained user objects.
		err = removeKubeLBSourceFinalizers(ctx, userClient)
		if err == nil {
			done, err = deleteCleanupResources(ctx, userClient, kubelbuserclusterresources.ResourcesForDeletion())
		}
		next = cleanupSeedCluster
	case cleanupSeedCluster:
		objects := kubelbseedresources.ResourcesForDeletion(cluster.Status.NamespaceName)
		objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: resources.KubeLBManagerKubeconfigSecretName, Namespace: cluster.Status.NamespaceName,
		}})
		done, err = deleteCleanupResources(ctx, r.Client, objects)
		next = ""
	default:
		return reconcile.Result{}, fmt.Errorf("unknown KubeLB cleanup phase %q", phase)
	}
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("KubeLB cleanup phase %q: %w", phase, err)
	}
	if !done {
		waitingFor := cleanupPhaseWaiting[phase]
		r.log.With("cluster", cluster.Name).Debugf("KubeLB cleanup phase %q is waiting for %s", phase, waitingFor)
		r.recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "KubeLBCleanupPending", "KubeLBCleanup",
			"Cleanup phase %q is waiting for %s", phase, waitingFor)
		return reconcile.Result{RequeueAfter: healthCheckPeriod}, nil
	}
	original := cluster.DeepCopy()
	if next == "" {
		delete(cluster.Annotations, cleanupPhaseAnnotation)
		kuberneteshelper.RemoveFinalizer(cluster, CleanupFinalizer)
	} else {
		if cluster.Annotations == nil {
			cluster.Annotations = map[string]string{}
		}
		cluster.Annotations[cleanupPhaseAnnotation] = next
	}
	if err := r.Patch(ctx, cluster, ctrlruntimeclient.MergeFromWithOptions(original, ctrlruntimeclient.MergeFromWithOptimisticLock{})); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to record KubeLB cleanup progress: %w", err)
	}
	if next == "" {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{RequeueAfter: healthCheckPeriod}, nil
}

func (r *reconciler) stopKubeLBCCM(ctx context.Context, namespace string) (bool, error) {
	deployment := &appsv1.Deployment{}
	key := ctrlruntimeclient.ObjectKey{Namespace: namespace, Name: resources.KubeLBDeploymentName}
	if err := r.Get(ctx, key, deployment); err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	if deployment.Name != "" && (deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0) {
		original := deployment.DeepCopy()
		deployment.Spec.Replicas = resources.Int32(0)
		if err := r.Patch(ctx, deployment, ctrlruntimeclient.MergeFromWithOptions(original, ctrlruntimeclient.MergeFromWithOptimisticLock{})); err != nil {
			return false, err
		}
		return false, nil
	}
	if deployment.Status.ObservedGeneration < deployment.Generation || deployment.Status.Replicas != 0 {
		return false, nil
	}
	// Check Pods as well as replicas: a terminating Pod can still reconcile.
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, ctrlruntimeclient.InNamespace(namespace), ctrlruntimeclient.MatchingLabels(resources.BaseAppLabels(resources.KubeLBDeploymentName, nil))); err != nil {
		return false, err
	}
	return len(pods.Items) == 0, nil
}

func removeKubeLBTenant(ctx context.Context, client ctrlruntimeclient.Client, name string) (bool, error) {
	done, err := deleteCleanupResources(ctx, client, kubelbmanagementresources.ResourcesForDeletion(name))
	if err != nil || !done {
		return done, err
	}
	// Tenant finalization can finish before namespace garbage collection. The
	// namespace must be gone before the KKP finalizer permits cluster teardown.
	namespace := &corev1.Namespace{}
	err = client.Get(ctx, ctrlruntimeclient.ObjectKey{Name: fmt.Sprintf(kubelbresources.TenantNamespacePattern, name)}, namespace)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	return false, err
}

func deleteCleanupResources(ctx context.Context, client ctrlruntimeclient.Client, objects []ctrlruntimeclient.Object) (bool, error) {
	done := true
	for _, obj := range objects {
		if err := client.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(obj), obj); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, err
		}
		done = false
		if obj.GetDeletionTimestamp() != nil {
			continue
		}
		// Foreground deletion also waits for dependent Pods/ReplicaSets. UID
		// preconditions prevent retries from deleting a replacement object.
		uid := obj.GetUID()
		err := client.Delete(ctx, obj, ctrlruntimeclient.PropagationPolicy(metav1.DeletePropagationForeground), &ctrlruntimeclient.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid},
		})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	return done, nil
}

func kubeLBObject(kind, namespace, name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("kubelb.k8c.io/v1alpha1")
	obj.SetKind(kind)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	return obj
}
