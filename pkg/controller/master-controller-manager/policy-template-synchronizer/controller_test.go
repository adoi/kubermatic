/*
Copyright 2025 The Kubermatic Kubernetes Platform contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package policytemplatesynchronizer

import (
	"context"
	"testing"
	"time"

	kubermaticv1 "k8c.io/kubermatic/sdk/v2/apis/kubermatic/v1"
	kubermaticlog "k8c.io/kubermatic/v2/pkg/log"
	"k8c.io/kubermatic/v2/pkg/test/diff"
	"k8c.io/kubermatic/v2/pkg/test/fake"
	"k8c.io/kubermatic/v2/pkg/test/generator"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const policyTemplateName = "policy-template-test"

func TestReconcile(t *testing.T) {
	testCases := []struct {
		name                   string
		requestName            string
		expectedPolicyTemplate *kubermaticv1.PolicyTemplate
		masterClient           ctrlruntimeclient.Client
		seedClient             ctrlruntimeclient.Client
		// expectBindingsPending, when true, expects that some PolicyBindings
		// still exist (marked for deletion) because the user-cluster controller
		// has not yet removed their finalizer.
		expectBindingsPending bool
	}{
		{
			name:                   "scenario 1: sync policy template from master cluster to seed cluster",
			requestName:            policyTemplateName,
			expectedPolicyTemplate: generatePolicyTemplate(policyTemplateName, false),
			masterClient: fake.
				NewClientBuilder().
				WithObjects(generatePolicyTemplate(policyTemplateName, false), generator.GenTestSeed()).
				Build(),
			seedClient: fake.
				NewClientBuilder().
				Build(),
		},
		{
			name:                   "scenario 2: cleanup policy template on the seed cluster when master cluster template is being terminated",
			requestName:            policyTemplateName,
			expectedPolicyTemplate: nil,
			masterClient: fake.
				NewClientBuilder().
				WithObjects(generatePolicyTemplate(policyTemplateName, true), generator.GenTestSeed()).
				Build(),
			seedClient: fake.
				NewClientBuilder().
				WithObjects(generatePolicyTemplate(policyTemplateName, false), generator.GenTestSeed()).
				Build(),
		},
		{
			name:                   "scenario 3: deletion cleans up PolicyBindings referencing the template",
			requestName:            policyTemplateName,
			expectedPolicyTemplate: nil,
			masterClient: fake.
				NewClientBuilder().
				WithObjects(generatePolicyTemplate(policyTemplateName, true), generator.GenTestSeed()).
				Build(),
			seedClient: fake.
				NewClientBuilder().
				WithObjects(
					generatePolicyTemplate(policyTemplateName, false),
					generateCluster("test-cluster", true),
					generatePolicyBinding("binding-1", "cluster-test-cluster", policyTemplateName),
					generatePolicyBinding("binding-2", "cluster-test-cluster", policyTemplateName),
				).
				Build(),
		},
		{
			name:                   "scenario 3b: deletion issues delete for PolicyBindings with finalizer on kyverno-enabled cluster",
			requestName:            policyTemplateName,
			expectedPolicyTemplate: nil,
			expectBindingsPending:  true,
			masterClient: fake.
				NewClientBuilder().
				WithObjects(generatePolicyTemplate(policyTemplateName, true), generator.GenTestSeed()).
				Build(),
			seedClient: fake.
				NewClientBuilder().
				WithObjects(
					generatePolicyTemplate(policyTemplateName, false),
					generateCluster("active-cluster", true),
					generatePolicyBindingWithFinalizer("active-binding", "cluster-active-cluster", policyTemplateName),
				).
				Build(),
		},
		{
			name:                   "scenario 4: deletion force-removes finalizer from orphaned PolicyBindings when cluster is gone",
			requestName:            policyTemplateName,
			expectedPolicyTemplate: nil,
			masterClient: fake.
				NewClientBuilder().
				WithObjects(generatePolicyTemplate(policyTemplateName, true), generator.GenTestSeed()).
				Build(),
			seedClient: fake.
				NewClientBuilder().
				WithObjects(
					generatePolicyTemplate(policyTemplateName, false),
					// No Cluster object — simulates a deleted cluster
					generatePolicyBindingWithFinalizer("orphan-binding", "cluster-gone-cluster", policyTemplateName),
				).
				Build(),
		},
		{
			name:                   "scenario 5: deletion force-removes finalizer when cluster exists with kyverno disabled",
			requestName:            policyTemplateName,
			expectedPolicyTemplate: nil,
			masterClient: fake.
				NewClientBuilder().
				WithObjects(generatePolicyTemplate(policyTemplateName, true), generator.GenTestSeed()).
				Build(),
			seedClient: fake.
				NewClientBuilder().
				WithObjects(
					generatePolicyTemplate(policyTemplateName, false),
					generateCluster("kyverno-disabled", false),
					generatePolicyBindingWithFinalizer("disabled-binding", "cluster-kyverno-disabled", policyTemplateName),
				).
				Build(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r := &reconciler{
				log:          kubermaticlog.Logger,
				recorder:     &events.FakeRecorder{},
				masterClient: tc.masterClient,
				seedClients:  map[string]ctrlruntimeclient.Client{"first": tc.seedClient},
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Name: tc.requestName}}
			if _, err := r.Reconcile(ctx, request); err != nil {
				t.Fatalf("reconciling failed: %v", err)
			}

			seedPolicyTemplate := &kubermaticv1.PolicyTemplate{}
			err := tc.seedClient.Get(ctx, request.NamespacedName, seedPolicyTemplate)
			if tc.expectedPolicyTemplate == nil {
				if err == nil {
					t.Fatal("failed clean up policy template on the seed cluster")
				} else if !apierrors.IsNotFound(err) {
					t.Fatalf("failed to get policy template: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("failed to get policy template: %v", err)
				}

				seedPolicyTemplate.ResourceVersion = ""
				seedPolicyTemplate.APIVersion = ""
				seedPolicyTemplate.Kind = ""

				if !diff.SemanticallyEqual(tc.expectedPolicyTemplate, seedPolicyTemplate) {
					t.Fatalf("Objects differ:\n%v", diff.ObjectDiff(tc.expectedPolicyTemplate, seedPolicyTemplate))
				}
			}

			// Verify PolicyBinding cleanup on seed.
			bindingList := &kubermaticv1.PolicyBindingList{}
			if err := tc.seedClient.List(ctx, bindingList); err != nil {
				t.Fatalf("failed to list PolicyBindings: %v", err)
			}
			for _, b := range bindingList.Items {
				if b.Spec.PolicyTemplateRef.Name != tc.requestName {
					continue
				}
				if !tc.expectBindingsPending {
					t.Fatalf("PolicyBinding %s/%s still references deleted PolicyTemplate", b.Namespace, b.Name)
				}
				// Binding is expected to still exist (finalizer held by user-cluster controller),
				// but it must have been marked for deletion.
				if b.DeletionTimestamp.IsZero() {
					t.Fatalf("PolicyBinding %s/%s should have DeletionTimestamp set", b.Namespace, b.Name)
				}
			}
		})
	}
}

func generatePolicyBinding(name, namespace, templateName string) *kubermaticv1.PolicyBinding {
	return &kubermaticv1.PolicyBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: kubermaticv1.PolicyBindingSpec{
			PolicyTemplateRef: corev1.ObjectReference{
				Name: templateName,
			},
		},
	}
}

func generatePolicyBindingWithFinalizer(name, namespace, templateName string) *kubermaticv1.PolicyBinding {
	binding := generatePolicyBinding(name, namespace, templateName)
	binding.Finalizers = []string{kubermaticv1.PolicyBindingCleanupFinalizer}
	return binding
}

func generateCluster(name string, kyvernoEnabled bool) *kubermaticv1.Cluster {
	return &kubermaticv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: kubermaticv1.ClusterSpec{
			Kyverno: &kubermaticv1.KyvernoSettings{
				Enabled: kyvernoEnabled,
			},
		},
		Status: kubermaticv1.ClusterStatus{
			NamespaceName: "cluster-" + name,
		},
	}
}

func generatePolicyTemplate(name string, deleted bool) *kubermaticv1.PolicyTemplate {
	pt := &kubermaticv1.PolicyTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: kubermaticv1.PolicyTemplateSpec{
			Title:       "Test Policy Template",
			Description: "Test Policy Template Description",
			Visibility:  "global",
			Enforced:    true,
			PolicySpec: runtime.RawExtension{
				Raw: []byte(`{"validationFailureAction":"Audit","background":true,"rules":[{"name":"test-rule","match":{"any":[{"resources":{"kinds":["Pod"]}}]},"validate":{"message":"Test validation message","pattern":{"metadata":{"labels":{"app.kubernetes.io/name":"?*"}}}}}]}`),
			},
		},
	}
	if deleted {
		deleteTime := metav1.NewTime(time.Now())
		pt.DeletionTimestamp = &deleteTime
		pt.Finalizers = append(pt.Finalizers, kubermaticv1.PolicyTemplateSeedCleanupFinalizer)
	}
	return pt
}
