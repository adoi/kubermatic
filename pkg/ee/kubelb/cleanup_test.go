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
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	kubermaticv1 "k8c.io/kubermatic/sdk/v2/apis/kubermatic/v1"
	kubelbseedresources "k8c.io/kubermatic/v2/pkg/ee/kubelb/resources/seed-cluster"
	kubelbuserclusterresources "k8c.io/kubermatic/v2/pkg/ee/kubelb/resources/user-cluster"
	"k8c.io/kubermatic/v2/pkg/resources"
	"k8c.io/kubermatic/v2/pkg/resources/apiserver"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlruntimefakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	cleanupTestNamespace       = "cluster-test"
	cleanupTestTenantNamespace = "tenant-test"
	otherFinalizer             = "example.com/other-controller"
)

func cleanupTestScheme(t *testing.T) (*runtime.Scheme, meta.RESTMapper) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, networkingv1.AddToScheme, rbacv1.AddToScheme, apiextensionsv1.AddToScheme, kubermaticv1.AddToScheme} {
		require.NoError(t, add(scheme))
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, networkingv1.SchemeGroupVersion, {Group: "kubelb.k8c.io", Version: "v1alpha1"}})
	for _, kind := range []string{"Service", "Secret"} {
		mapper.Add(corev1.SchemeGroupVersion.WithKind(kind), meta.RESTScopeNamespace)
	}
	mapper.Add(networkingv1.SchemeGroupVersion.WithKind("Ingress"), meta.RESTScopeNamespace)
	for _, kind := range []string{"Tenant", "TenantState", "TenantWAFPolicy", "SyncSecret"} {
		obj := kubeLBObject(kind, "", "")
		scheme.AddKnownTypeWithName(obj.GroupVersionKind(), obj)
		list := obj.GroupVersionKind().GroupVersion().WithKind(kind + "List")
		scheme.AddKnownTypeWithName(list, &unstructured.UnstructuredList{})
		scope := meta.RESTScopeNamespace
		if kind == "Tenant" {
			scope = meta.RESTScopeRoot
		}
		mapper.Add(obj.GroupVersionKind(), scope)
	}
	return scheme, mapper
}

func cleanupTestClient(t *testing.T, objects ...ctrlruntimeclient.Object) ctrlruntimeclient.WithWatch {
	t.Helper()
	scheme, mapper := cleanupTestScheme(t)
	return ctrlruntimefakeclient.NewClientBuilder().WithScheme(scheme).WithRESTMapper(mapper).WithObjects(objects...).Build()
}

func cleanupTestReconciler(client ctrlruntimeclient.Client) *reconciler {
	return &reconciler{Client: client, log: zap.NewNop().Sugar(), recorder: events.NewFakeRecorder(128)}
}

func cleanupTestCluster(phase string) *kubermaticv1.Cluster {
	return &kubermaticv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test", Finalizers: []string{CleanupFinalizer, otherFinalizer},
			Annotations: map[string]string{cleanupPhaseAnnotation: phase, "example.com/preserve": "yes"},
		},
		Status: kubermaticv1.ClusterStatus{NamespaceName: cleanupTestNamespace},
	}
}

func cleanupTestDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: resources.KubeLBDeploymentName, Namespace: cleanupTestNamespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](1),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: resources.KubeLBDeploymentName, Image: "quay.io/kubermatic/kubelb-ccm-ee:v1.4.3",
				Command: []string{"/ccm"},
				Args:    []string{"-leader-election-namespace", metav1.NamespaceSystem},
			}}}},
		},
	}
}

func reconcileCleanupTest(t *testing.T, r *reconciler, user, management, tenant ctrlruntimeclient.Client) reconcile.Result {
	t.Helper()
	cluster := &kubermaticv1.Cluster{}
	require.NoError(t, r.Get(context.Background(), ctrlruntimeclient.ObjectKey{Name: "test"}, cluster))
	result, err := r.reconcileKubeLBCleanup(context.Background(), cluster, user, management, func(context.Context) (ctrlruntimeclient.Client, error) { return tenant, nil })
	require.NoError(t, err)
	return result
}

func assertCleanupPhase(t *testing.T, client ctrlruntimeclient.Client, phase string) {
	t.Helper()
	cluster := &kubermaticv1.Cluster{}
	require.NoError(t, client.Get(context.Background(), ctrlruntimeclient.ObjectKey{Name: "test"}, cluster))
	require.Equal(t, phase, cluster.Annotations[cleanupPhaseAnnotation])
	if phase != "" {
		require.Contains(t, cluster.Finalizers, CleanupFinalizer)
	}
}

func assertObjectExists(t *testing.T, client ctrlruntimeclient.Client, object ctrlruntimeclient.Object) {
	t.Helper()
	require.NoError(t, client.Get(context.Background(), ctrlruntimeclient.ObjectKeyFromObject(object), object))
}

func TestKubeLBCleanupLifecycle(t *testing.T) {
	ctx := context.Background()
	cluster := cleanupTestCluster("")
	deployment := cleanupTestDeployment()
	seedObjects := kubelbseedresources.ResourcesForDeletion(cleanupTestNamespace)
	seedObjects[0] = deployment
	seedObjects = append(seedObjects, cluster,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeLBManagerKubeconfigSecretName, Namespace: cleanupTestNamespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeLBCCMKubeconfigSecretName, Namespace: cleanupTestNamespace}},
	)
	seed := cleanupTestClient(t, seedObjects...)
	r := cleanupTestReconciler(seed)

	policy := kubeLBObject("TenantWAFPolicy", "application", "waf")
	policy.SetFinalizers([]string{kubeLBSourceFinalizer})
	proxy := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName, Namespace: metav1.NamespaceSystem, Finalizers: []string{otherFinalizer}}}
	proxySecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "kubelb-tenant-proxy-server-tls", Namespace: metav1.NamespaceSystem}}
	source := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "application", Finalizers: []string{kubeLBSourceFinalizer, otherFinalizer}}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer}}
	userObjects := append(kubelbuserclusterresources.ResourcesForDeletion(), policy, proxy, proxySecret, source)
	user := cleanupTestClient(t, userObjects...)
	tenant := kubeLBObject("Tenant", "", "test")
	tenant.SetFinalizers([]string{kubeLBSourceFinalizer})
	state := kubeLBObject("TenantState", cleanupTestTenantNamespace, "default")
	state.SetFinalizers([]string{tenantProxyFinalizer, otherFinalizer})
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cleanupTestTenantNamespace}}
	management := cleanupTestClient(t, tenant, state, namespace)

	require.NotZero(t, reconcileCleanupTest(t, r, user, management, management).RequeueAfter)
	assertCleanupPhase(t, seed, cleanupDrainWAF)
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, seed, cleanupDrainWAF)
	assertObjectExists(t, user, policy)
	require.NotNil(t, policy.GetDeletionTimestamp())
	assertObjectExists(t, seed, deployment)
	require.EqualValues(t, 1, *deployment.Spec.Replicas)
	assertObjectExists(t, management, tenant)
	require.Nil(t, tenant.GetDeletionTimestamp())

	// Simulate CCM completing the WAF policy's cleanup; KKP does not strip it.
	policy.SetFinalizers(nil)
	require.NoError(t, user.Update(ctx, policy))
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, seed, cleanupStopCCM)
	reconcileCleanupTest(t, r, user, management, management)
	assertObjectExists(t, seed, deployment)
	require.Zero(t, *deployment.Spec.Replicas)
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, seed, cleanupProxy)

	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, seed, cleanupProxy)
	assertObjectExists(t, user, proxy)
	require.NotNil(t, proxy.DeletionTimestamp)
	assertObjectExists(t, user, proxySecret)
	assertObjectExists(t, management, state)
	require.Contains(t, state.GetFinalizers(), tenantProxyFinalizer)

	// Simulate asynchronous workload deletion. Retry using a fresh reconciler
	// to exercise the persisted state after a controller restart.
	proxy.Finalizers = nil
	require.NoError(t, user.Update(ctx, proxy))
	r = cleanupTestReconciler(seed)
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, seed, cleanupProxy)
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, seed, cleanupTenant)
	assertObjectExists(t, management, state)
	require.Equal(t, []string{otherFinalizer}, state.GetFinalizers())

	reconcileCleanupTest(t, r, user, management, management)
	assertObjectExists(t, management, tenant)
	require.NotNil(t, tenant.GetDeletionTimestamp())
	assertCleanupPhase(t, seed, cleanupTenant)
	assertObjectExists(t, user, source)
	require.Contains(t, source.Finalizers, kubeLBSourceFinalizer)
	for _, obj := range kubelbuserclusterresources.ResourcesForDeletion() {
		assertObjectExists(t, user, obj)
	}
	assertObjectExists(t, seed, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeLBManagerKubeconfigSecretName, Namespace: cleanupTestNamespace}})

	// The Tenant CR disappearing alone is insufficient: namespace finalizers
	// and management mirrors must have finished too.
	tenant.SetFinalizers(nil)
	require.NoError(t, management.Update(ctx, tenant))
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, seed, cleanupTenant)
	require.NoError(t, management.Delete(ctx, namespace))
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, seed, cleanupUserCluster)

	reconcileCleanupTest(t, r, user, management, management)
	assertObjectExists(t, user, source)
	require.Equal(t, []string{otherFinalizer}, source.Finalizers)
	require.Equal(t, corev1.ServiceTypeLoadBalancer, source.Spec.Type)
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, seed, cleanupSeedCluster)
	reconcileCleanupTest(t, r, user, management, management)
	require.Zero(t, reconcileCleanupTest(t, r, user, management, management).RequeueAfter)
	assertObjectExists(t, seed, cluster)
	require.NotContains(t, cluster.Finalizers, CleanupFinalizer)
	require.Equal(t, []string{otherFinalizer}, cluster.Finalizers)
	require.Equal(t, "yes", cluster.Annotations["example.com/preserve"])
	require.NotContains(t, cluster.Annotations, cleanupPhaseAnnotation)
	// The internal user API credential is owned by the Kubernetes controller.
	assertObjectExists(t, seed, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeLBCCMKubeconfigSecretName, Namespace: cleanupTestNamespace}})
}

func TestDrainTenantWAFPoliciesMissingAPIAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		wantDone bool
	}{
		{"missing CRD", &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "kubelb.k8c.io", Kind: "TenantWAFPolicy"}}, true},
		{"removed API", apierrors.NewNotFound(schema.GroupResource{Group: "kubelb.k8c.io", Resource: "tenantwafpolicies"}, ""), true},
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{Group: "kubelb.k8c.io", Resource: "tenantwafpolicies"}, "", errors.New("denied")), false},
		{"unavailable", errors.New("API unavailable"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := interceptor.NewClient(cleanupTestClient(t), interceptor.Funcs{List: func(context.Context, ctrlruntimeclient.WithWatch, ctrlruntimeclient.ObjectList, ...ctrlruntimeclient.ListOption) error {
				return tc.err
			}})
			done, err := drainTenantWAFPolicies(context.Background(), client)
			require.Equal(t, tc.wantDone, done)
			if tc.wantDone {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.err)
			}
		})
	}
}

func TestStopKubeLBCCMWaitsForControllerAndPods(t *testing.T) {
	ctx := context.Background()
	deployment := cleanupTestDeployment()
	deployment.Spec.Replicas = ptr.To[int32](0)
	deployment.Generation = 2
	deployment.Status.ObservedGeneration = 1
	client := cleanupTestClient(t, deployment)
	r := cleanupTestReconciler(client)
	done, err := r.stopKubeLBCCM(ctx, cleanupTestNamespace)
	require.NoError(t, err)
	require.False(t, done)
	deployment.Status.ObservedGeneration = 2
	require.NoError(t, client.Status().Update(ctx, deployment))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ccm-pod", Namespace: cleanupTestNamespace, Labels: resources.BaseAppLabels(resources.KubeLBDeploymentName, nil), Finalizers: []string{otherFinalizer}}}
	require.NoError(t, client.Create(ctx, pod))
	require.NoError(t, client.Delete(ctx, pod))
	done, err = r.stopKubeLBCCM(ctx, cleanupTestNamespace)
	require.NoError(t, err)
	require.False(t, done, "a terminating CCM Pod may still reconcile")
	assertObjectExists(t, client, pod)
	pod.Finalizers = nil
	require.NoError(t, client.Update(ctx, pod))
	done, err = r.stopKubeLBCCM(ctx, cleanupTestNamespace)
	require.NoError(t, err)
	require.True(t, done)
}

func TestCleanupContinuesAfterReenable(t *testing.T) {
	cluster := cleanupTestCluster(cleanupStopCCM)
	cluster.Spec.KubeLB = &kubermaticv1.KubeLB{Enabled: true}
	seed := cleanupTestClient(t, cluster, cleanupTestDeployment())
	r := cleanupTestReconciler(seed)
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: cluster.Name}})
	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
	deployment := cleanupTestDeployment()
	assertObjectExists(t, seed, deployment)
	require.Zero(t, *deployment.Spec.Replicas)
	assertCleanupPhase(t, seed, cleanupStopCCM)
	require.NotEmpty(t, r.recorder.(*events.FakeRecorder).Events)
	require.Contains(t, <-r.recorder.(*events.FakeRecorder).Events, `Cleanup phase "stop-ccm" is waiting for`)
}

func TestCleanupFailureRetainsProgressAndFinalizer(t *testing.T) {
	cluster := cleanupTestCluster(cleanupTenant)
	seed := cleanupTestClient(t, cluster)
	failed := errors.New("management cluster unavailable")
	management := interceptor.NewClient(cleanupTestClient(t), interceptor.Funcs{Get: func(context.Context, ctrlruntimeclient.WithWatch, ctrlruntimeclient.ObjectKey, ctrlruntimeclient.Object, ...ctrlruntimeclient.GetOption) error {
		return failed
	}})
	r := cleanupTestReconciler(seed)
	_, err := r.reconcileKubeLBCleanup(context.Background(), cluster, nil, management, nil)
	require.ErrorIs(t, err, failed)
	assertCleanupPhase(t, seed, cleanupTenant)
}

func TestTenantProxySettings(t *testing.T) {
	for _, tc := range []struct {
		name          string
		args          []string
		env           []corev1.EnvVar
		wantNamespace string
		wantCustom    bool
	}{
		{name: "defaults", wantNamespace: "kube-system"},
		{name: "overridden leader namespace", args: []string{"-leader-election-namespace", "kube-system", "--leader-election-namespace=proxy"}, wantNamespace: "proxy"},
		{name: "empty leader namespace", args: []string{"--leader-election-namespace="}, wantNamespace: "default"},
		{name: "literal environment wins", args: []string{"--leader-election-namespace=proxy"}, env: []corev1.EnvVar{{Name: "NAMESPACE", Value: "tenant-system"}}, wantNamespace: "tenant-system"},
		{name: "custom service account", args: []string{"--tenant-proxy-service-account-name=custom"}, wantNamespace: "kube-system", wantCustom: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, wrapped := range []bool{false, true} {
				t.Run(fmt.Sprintf("wrapped=%t", wrapped), func(t *testing.T) {
					deployment := cleanupTestDeployment()
					deployment.Spec.Template.Spec.Containers[0].Args = tc.args
					deployment.Spec.Template.Spec.Containers[0].Env = tc.env
					if wrapped {
						wrapCleanupTestDeployment(t, deployment)
					}
					namespace, custom, err := tenantProxySettings(deployment)
					require.NoError(t, err)
					require.Equal(t, tc.wantNamespace, namespace)
					require.Equal(t, tc.wantCustom, custom)
				})
			}
		})
	}
}

func wrapCleanupTestDeployment(t *testing.T, deployment *appsv1.Deployment) {
	t.Helper()
	data := resources.NewTemplateDataBuilder().WithCluster(cleanupTestCluster("")).Build()
	template, err := apiserver.IsRunningWrapper(data, deployment.Spec.Template, sets.New(resources.KubeLBDeploymentName))
	require.NoError(t, err)
	deployment.Spec.Template = template
}

func TestProxyCleanupUsesWrappedNamespace(t *testing.T) {
	ctx := context.Background()
	cluster := cleanupTestCluster(cleanupProxy)
	deployment := cleanupTestDeployment()
	deployment.Spec.Template.Spec.Containers[0].Args = []string{"--leader-election-namespace=proxy-system", "--tenant-proxy-service-account-name=" + tenantProxyName}
	wrapCleanupTestDeployment(t, deployment)
	r := cleanupTestReconciler(cleanupTestClient(t, cluster, deployment))
	proxy := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName, Namespace: "proxy-system", Finalizers: []string{otherFinalizer}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "kubelb-tenant-proxy-server-tls", Namespace: "proxy-system"}}
	customSA := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName, Namespace: "proxy-system"}}
	untouched := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName, Namespace: "kube-system"}}
	user := cleanupTestClient(t, proxy, secret, customSA, untouched)
	state := kubeLBObject("TenantState", cleanupTestTenantNamespace, "default")
	state.SetFinalizers([]string{tenantProxyFinalizer, otherFinalizer})
	management := cleanupTestClient(t, state)

	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, r.Client, cleanupProxy)
	assertObjectExists(t, user, proxy)
	require.NotNil(t, proxy.DeletionTimestamp)
	assertObjectExists(t, user, secret)
	assertObjectExists(t, management, state)
	require.Contains(t, state.GetFinalizers(), tenantProxyFinalizer)
	proxy.Finalizers = nil
	require.NoError(t, user.Update(ctx, proxy))
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, r.Client, cleanupProxy)
	reconcileCleanupTest(t, r, user, management, management)
	assertCleanupPhase(t, r.Client, cleanupTenant)
	assertObjectExists(t, management, state)
	require.Equal(t, []string{otherFinalizer}, state.GetFinalizers())
	assertObjectExists(t, user, customSA)
	assertObjectExists(t, user, untouched)
	require.Nil(t, untouched.DeletionTimestamp)
}

func TestProxyCleanupRejectsInvalidWrappedCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"-command"}, {"-command", "invalid-json"}, {"-command", "{}"}} {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			cluster := cleanupTestCluster(cleanupProxy)
			deployment := cleanupTestDeployment()
			wrapCleanupTestDeployment(t, deployment)
			deployment.Spec.Template.Spec.Containers[0].Args = args
			r := cleanupTestReconciler(cleanupTestClient(t, cluster, deployment))
			proxy := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName, Namespace: "kube-system"}}
			user := cleanupTestClient(t, proxy)
			// No management credentials should be accessed with unknown settings.
			_, err := r.reconcileKubeLBCleanup(context.Background(), cluster, user, nil, nil)
			require.ErrorContains(t, err, "invalid CCM http-prober command")
			assertCleanupPhase(t, r.Client, cleanupProxy)
			assertObjectExists(t, user, proxy)
			require.Nil(t, proxy.DeletionTimestamp)
		})
	}
}

func TestProxyCleanupDeletesOrphans(t *testing.T) {
	ctx := context.Background()
	for _, withReplicaSet := range []bool{false, true} {
		t.Run(fmt.Sprintf("replicaset=%t", withReplicaSet), func(t *testing.T) {
			cluster := cleanupTestCluster(cleanupProxy)
			r := cleanupTestReconciler(cleanupTestClient(t, cluster, cleanupTestDeployment()))
			labels := map[string]string{"app.kubernetes.io/name": tenantProxyName, "kubelb.k8c.io/tenant": cleanupTestTenantNamespace}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "proxy-pod", Namespace: "kube-system", UID: "pod-uid", Labels: labels}}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "kubelb-tenant-proxy-server-tls", Namespace: "kube-system"}}
			objects := []ctrlruntimeclient.Object{pod, secret}
			if withReplicaSet {
				rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: tenantProxyName + "-abc", Namespace: "kube-system", UID: "rs-uid", Labels: labels}}
				pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}
				objects = append(objects, rs)
			}
			unrelated := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "kube-system", Labels: map[string]string{"app.kubernetes.io/name": tenantProxyName, "kubelb.k8c.io/tenant": "another-tenant"}}}
			user := interceptor.NewClient(cleanupTestClient(t, append(objects, unrelated)...), interceptor.Funcs{
				Delete: func(ctx context.Context, client ctrlruntimeclient.WithWatch, object ctrlruntimeclient.Object, opts ...ctrlruntimeclient.DeleteOption) error {
					switch object.(type) {
					case *corev1.Pod, *appsv1.ReplicaSet:
						options := (&ctrlruntimeclient.DeleteOptions{}).ApplyOptions(opts)
						require.Equal(t, object.GetUID(), *options.Preconditions.UID)
						require.NotEmpty(t, *options.Preconditions.ResourceVersion)
						require.Equal(t, object.GetResourceVersion(), *options.Preconditions.ResourceVersion)
						require.Equal(t, metav1.DeletePropagationForeground, *options.PropagationPolicy)
						assertObjectExists(t, client, secret)
					}
					if _, pod := object.(*corev1.Pod); pod && withReplicaSet {
						require.True(t, apierrors.IsNotFound(client.Get(ctx, ctrlruntimeclient.ObjectKey{Namespace: "kube-system", Name: tenantProxyName + "-abc"}, &appsv1.ReplicaSet{})))
					}
					return client.Delete(ctx, object, opts...)
				},
			})
			getTenant := func(context.Context) (ctrlruntimeclient.Client, error) { return nil, nil }
			for range 8 {
				done, err := r.cleanupTenantProxy(ctx, cluster, user, getTenant)
				require.NoError(t, err)
				if done {
					assertObjectExists(t, user, unrelated)
					for _, object := range objects {
						require.True(t, apierrors.IsNotFound(user.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(object), object)))
					}
					return
				}
			}
			t.Fatal("cleanup never deleted the orphaned proxy workload")
		})
	}
}

func TestProxyOrphanCleanupChecksController(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		ownerName  string
		ownerUID   types.UID
		liveOwner  bool
		getError   bool
		wantDelete bool
		wantError  bool
	}{
		{name: "missing daemonset", ownerName: tenantProxyName, ownerUID: "old", wantDelete: true},
		{name: "live owner", ownerName: tenantProxyName, ownerUID: "live", liveOwner: true},
		{name: "replacement owner", ownerName: tenantProxyName, ownerUID: "old", liveOwner: true, wantDelete: true},
		{name: "unrelated controller", ownerName: "unrelated", ownerUID: "old", wantError: true},
		{name: "owner lookup fails", ownerName: tenantProxyName, ownerUID: "old", getError: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: tc.ownerName, Namespace: "kube-system", UID: tc.ownerUID}}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "proxy-pod", Namespace: "kube-system", UID: "pod", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(owner, appsv1.SchemeGroupVersion.WithKind("DaemonSet"))}}}
			base := cleanupTestClient(t, pod)
			if tc.liveOwner {
				owner.UID = "live"
				require.NoError(t, base.Create(ctx, owner))
			}
			assertObjectExists(t, base, pod)
			client := interceptor.NewClient(base, interceptor.Funcs{
				Get: func(ctx context.Context, client ctrlruntimeclient.WithWatch, key ctrlruntimeclient.ObjectKey, object ctrlruntimeclient.Object, opts ...ctrlruntimeclient.GetOption) error {
					if tc.getError {
						return errors.New("owner lookup unavailable")
					}
					return client.Get(ctx, key, object, opts...)
				},
			})
			done, err := deleteOrphanedTenantProxyResources(ctx, client, []ctrlruntimeclient.Object{pod})
			require.False(t, done)
			require.Equal(t, tc.wantError, err != nil)
			require.Equal(t, tc.wantDelete, apierrors.IsNotFound(base.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(pod), pod)))
		})
	}
}

func TestProxyCleanupWaitsAndPreservesCredentials(t *testing.T) {
	ctx := context.Background()
	cluster := cleanupTestCluster(cleanupProxy)
	deployment := cleanupTestDeployment()
	deployment.Spec.Replicas = ptr.To[int32](0)
	deployment.Spec.Template.Spec.Containers[0].Args = append(deployment.Spec.Template.Spec.Containers[0].Args, "--tenant-proxy-service-account-name=external")
	r := cleanupTestReconciler(cleanupTestClient(t, cluster, deployment))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "proxy-pod", Namespace: "kube-system", DeletionTimestamp: ptr.To(metav1.Now()), Finalizers: []string{otherFinalizer}, Labels: map[string]string{"app.kubernetes.io/name": tenantProxyName, "kubelb.k8c.io/tenant": cleanupTestTenantNamespace}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "kubelb-tenant-proxy-server-tls", Namespace: "kube-system"}}
	externalSA := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "external", Namespace: "kube-system"}}
	user := cleanupTestClient(t, pod, secret, externalSA)
	tenantClientCalled := false
	getTenant := func(context.Context) (ctrlruntimeclient.Client, error) { tenantClientCalled = true; return nil, nil }
	done, err := r.cleanupTenantProxy(ctx, cluster, user, getTenant)
	require.NoError(t, err)
	require.False(t, done)
	require.False(t, tenantClientCalled)
	assertObjectExists(t, user, secret)
	assertObjectExists(t, user, pod)
	pod.Finalizers = nil
	require.NoError(t, user.Update(ctx, pod))
	done, err = r.cleanupTenantProxy(ctx, cluster, user, getTenant)
	require.NoError(t, err)
	require.False(t, done)
	done, err = r.cleanupTenantProxy(ctx, cluster, user, getTenant)
	require.NoError(t, err)
	require.True(t, done)
	assertObjectExists(t, user, externalSA)
}

func TestSourceFinalizersPreserveUserResources(t *testing.T) {
	ctx := context.Background()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls", Namespace: "app", Finalizers: []string{kubeLBSourceFinalizer, otherFinalizer}}, Data: map[string][]byte{"tls.crt": []byte("preserve")}}
	ingress := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app", Finalizers: []string{kubeLBSourceFinalizer, otherFinalizer}}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app", Finalizers: []string{kubeLBLegacyServiceFinalizer, otherFinalizer}}}
	syncSecret := kubeLBObject("SyncSecret", "app", "sync")
	syncSecret.SetFinalizers([]string{kubeLBSourceFinalizer, otherFinalizer})
	user := cleanupTestClient(t, secret, ingress, service, syncSecret)
	for range 2 {
		require.NoError(t, removeKubeLBSourceFinalizers(ctx, user))
		for _, obj := range []ctrlruntimeclient.Object{secret, ingress, service, syncSecret} {
			assertObjectExists(t, user, obj)
			require.Equal(t, []string{otherFinalizer}, obj.GetFinalizers(), fmt.Sprintf("%T", obj))
		}
		require.Equal(t, []byte("preserve"), secret.Data["tls.crt"])
	}
}

func TestCleanupWithV143APIs(t *testing.T) {
	ctx := context.Background()
	cluster := cleanupTestCluster("")
	deployment := cleanupTestDeployment()
	seed := cleanupTestClient(t, cluster, deployment)
	r := cleanupTestReconciler(seed)
	missing := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "kubelb.k8c.io", Kind: "TenantWAFPolicy"}}
	user := interceptor.NewClient(cleanupTestClient(t, kubelbuserclusterresources.ResourcesForDeletion()...), interceptor.Funcs{
		List: func(ctx context.Context, client ctrlruntimeclient.WithWatch, list ctrlruntimeclient.ObjectList, opts ...ctrlruntimeclient.ListOption) error {
			if list.GetObjectKind().GroupVersionKind().Kind == "TenantWAFPolicyList" {
				return missing
			}
			return client.List(ctx, list, opts...)
		},
	})
	management := cleanupTestClient(t)
	tenant := interceptor.NewClient(management, interceptor.Funcs{
		Get: func(context.Context, ctrlruntimeclient.WithWatch, ctrlruntimeclient.ObjectKey, ctrlruntimeclient.Object, ...ctrlruntimeclient.GetOption) error {
			return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "kubelb.k8c.io", Kind: "TenantState"}}
		},
	})
	for range 20 {
		reconcileCleanupTest(t, r, user, management, tenant)
		require.NoError(t, seed.Get(ctx, ctrlruntimeclient.ObjectKey{Name: cluster.Name}, cluster))
		if !slices.Contains(cluster.Finalizers, CleanupFinalizer) {
			return
		}
	}
	t.Fatal("cleanup did not complete without the v1.5 APIs")
}

func TestFinalizerCleanupPreservesConcurrentChanges(t *testing.T) {
	ctx := context.Background()
	source := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app", Finalizers: []string{kubeLBSourceFinalizer}}}
	base := cleanupTestClient(t, source)
	attempts := 0
	client := interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(ctx context.Context, client ctrlruntimeclient.WithWatch, object ctrlruntimeclient.Object, patch ctrlruntimeclient.Patch, opts ...ctrlruntimeclient.PatchOption) error {
			attempts++
			if attempts == 1 {
				current := &corev1.Service{}
				require.NoError(t, client.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(object), current))
				current.Finalizers = append(current.Finalizers, otherFinalizer)
				require.NoError(t, client.Update(ctx, current))
			}
			return client.Patch(ctx, object, patch, opts...)
		},
	})
	require.NoError(t, removeKubeLBFinalizers(ctx, client, source, kubeLBSourceFinalizer))
	require.Equal(t, 2, attempts)
	assertObjectExists(t, base, source)
	require.Equal(t, []string{otherFinalizer}, source.Finalizers)
}

func TestProxyFinalizerFailureDoesNotDeleteTenant(t *testing.T) {
	ctx := context.Background()
	cluster := cleanupTestCluster(cleanupProxy)
	r := cleanupTestReconciler(cleanupTestClient(t, cluster))
	state := kubeLBObject("TenantState", cleanupTestTenantNamespace, "default")
	state.SetFinalizers([]string{tenantProxyFinalizer})
	management := cleanupTestClient(t, state)
	failed := apierrors.NewForbidden(schema.GroupResource{Group: "kubelb.k8c.io", Resource: "tenantstates"}, "default", errors.New("denied"))
	scoped := interceptor.NewClient(management, interceptor.Funcs{
		Patch: func(context.Context, ctrlruntimeclient.WithWatch, ctrlruntimeclient.Object, ctrlruntimeclient.Patch, ...ctrlruntimeclient.PatchOption) error {
			return failed
		},
	})
	_, err := r.reconcileKubeLBCleanup(ctx, cluster, cleanupTestClient(t), management, func(context.Context) (ctrlruntimeclient.Client, error) { return scoped, nil })
	require.ErrorIs(t, err, failed)
	assertCleanupPhase(t, r.Client, cleanupProxy)
	assertObjectExists(t, management, state)
	require.Contains(t, state.GetFinalizers(), tenantProxyFinalizer)
}

func TestDeletionWaitsAndUsesUIDPreconditions(t *testing.T) {
	ctx := context.Background()
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "proxy", Namespace: "app", UID: "original", Finalizers: []string{otherFinalizer}}}
	client := interceptor.NewClient(cleanupTestClient(t, service), interceptor.Funcs{
		Delete: func(ctx context.Context, client ctrlruntimeclient.WithWatch, object ctrlruntimeclient.Object, opts ...ctrlruntimeclient.DeleteOption) error {
			options := &ctrlruntimeclient.DeleteOptions{}
			options.ApplyOptions(opts)
			require.Equal(t, types.UID("original"), *options.Preconditions.UID)
			require.Equal(t, metav1.DeletePropagationForeground, *options.PropagationPolicy)
			return client.Delete(ctx, object, opts...)
		},
	})
	for range 2 {
		done, err := deleteCleanupResources(ctx, client, []ctrlruntimeclient.Object{service})
		require.NoError(t, err)
		require.False(t, done)
	}
	assertObjectExists(t, client, service)
	require.Equal(t, []string{otherFinalizer}, service.Finalizers)
}

func TestTenantCleanupCredentials(t *testing.T) {
	ctx := context.Background()
	cluster := cleanupTestCluster(cleanupProxy)
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cleanupTestTenantNamespace}}
	kubeconfig := []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://management.example.invalid
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user:
    token: test-only
`)
	for _, tc := range []struct {
		name                           string
		seedObjects, managementObjects []ctrlruntimeclient.Object
		wantClient, wantError          bool
	}{
		{name: "no tenant namespace"},
		{name: "existing seed credentials", seedObjects: []ctrlruntimeclient.Object{&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeLBManagerKubeconfigSecretName, Namespace: cleanupTestNamespace}, Data: map[string][]byte{resources.KubeconfigSecretKey: kubeconfig}}}, managementObjects: []ctrlruntimeclient.Object{namespace}, wantClient: true},
		{name: "interrupted installation uses manager credentials", managementObjects: []ctrlruntimeclient.Object{namespace, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: kubeLBCCMKubeconfigSecretName, Namespace: cleanupTestTenantNamespace}, Data: map[string][]byte{kubeconfigSecretKey: kubeconfig}}}, wantClient: true},
		{name: "missing credentials retain finalizer", managementObjects: []ctrlruntimeclient.Object{namespace}, wantError: true},
		{name: "empty credentials are rejected", seedObjects: []ctrlruntimeclient.Object{&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeLBManagerKubeconfigSecretName, Namespace: cleanupTestNamespace}}}, managementObjects: []ctrlruntimeclient.Object{namespace}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := cleanupTestReconciler(cleanupTestClient(t, tc.seedObjects...))
			client, err := r.getTenantCleanupClient(ctx, cluster, cleanupTestClient(t, tc.managementObjects...), kubeconfig)
			require.Equal(t, tc.wantError, err != nil)
			require.Equal(t, tc.wantClient, client != nil)
		})
	}
}
