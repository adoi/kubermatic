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
	"fmt"
	"strings"

	"go.uber.org/zap"

	kubermaticv1 "k8c.io/kubermatic/sdk/v2/apis/kubermatic/v1"
	kuberneteshelper "k8c.io/kubermatic/v2/pkg/kubernetes"
	"k8c.io/kubermatic/v2/pkg/resources/reconciling"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	ControllerName = "kkp-policy-template-synchronizer"
)

type reconciler struct {
	log          *zap.SugaredLogger
	recorder     events.EventRecorder
	masterClient ctrlruntimeclient.Client
	seedClients  kuberneteshelper.SeedClientMap
}

func Add(
	masterManager manager.Manager,
	seedManagers map[string]manager.Manager,
	log *zap.SugaredLogger,
	numWorkers int,
) error {
	r := &reconciler{
		log:          log.Named(ControllerName),
		recorder:     masterManager.GetEventRecorder(ControllerName),
		masterClient: masterManager.GetClient(),
		seedClients:  kuberneteshelper.SeedClientMap{},
	}

	for seedName, seedManager := range seedManagers {
		r.seedClients[seedName] = seedManager.GetClient()
	}

	_, err := builder.ControllerManagedBy(masterManager).
		Named(ControllerName).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: numWorkers,
		}).
		For(&kubermaticv1.PolicyTemplate{}).
		Build(r)

	return err
}

// Reconcile reconciles PolicyTemplate objects from master cluster to all seed clusters.
func (r *reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.log.With("policytemplate", request.Name)
	log.Debug("Processing")

	policyTemplate := &kubermaticv1.PolicyTemplate{}
	if err := r.masterClient.Get(ctx, request.NamespacedName, policyTemplate); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	err := r.reconcile(ctx, log, policyTemplate)
	if err != nil {
		r.recorder.Eventf(policyTemplate, nil, corev1.EventTypeWarning, "ReconcilingError", "Reconciling", err.Error())
	}

	return reconcile.Result{}, err
}

func (r *reconciler) reconcile(ctx context.Context, log *zap.SugaredLogger, policyTemplate *kubermaticv1.PolicyTemplate) error {
	// handling deletion
	if !policyTemplate.DeletionTimestamp.IsZero() {
		if err := r.handleDeletion(ctx, log, policyTemplate); err != nil {
			return fmt.Errorf("failed to handle deletion of policy template: %w", err)
		}
		return nil
	}

	// add the cleanup finalizer
	if !kuberneteshelper.HasFinalizer(policyTemplate, kubermaticv1.PolicyTemplateSeedCleanupFinalizer) {
		if err := kuberneteshelper.TryAddFinalizer(ctx, r.masterClient, policyTemplate, kubermaticv1.PolicyTemplateSeedCleanupFinalizer); err != nil {
			return fmt.Errorf("failed to add finalizer: %w", err)
		}
	}

	policyTemplateReconcilerFactories := []reconciling.NamedPolicyTemplateReconcilerFactory{
		policyTemplateReconcilerFactory(policyTemplate),
	}

	err := r.seedClients.Each(ctx, log, func(_ string, seedClient ctrlruntimeclient.Client, log *zap.SugaredLogger) error {
		seedPolicyTemplate := &kubermaticv1.PolicyTemplate{}
		if err := seedClient.Get(ctx, ctrlruntimeclient.ObjectKeyFromObject(policyTemplate), seedPolicyTemplate); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to fetch PolicyTemplate on seed cluster: %w", err)
		}

		if seedPolicyTemplate.UID != "" && seedPolicyTemplate.UID == policyTemplate.UID {
			return nil
		}
		return reconciling.ReconcilePolicyTemplates(ctx, policyTemplateReconcilerFactories, "", seedClient)
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile policy template %q across seeds: %w", policyTemplate.Name, err)
	}
	return nil
}

func (r *reconciler) handleDeletion(ctx context.Context, log *zap.SugaredLogger, policyTemplate *kubermaticv1.PolicyTemplate) error {
	if !kuberneteshelper.HasFinalizer(policyTemplate, kubermaticv1.PolicyTemplateSeedCleanupFinalizer) {
		return nil
	}

	// Clean up PolicyBindings referencing this template on all seeds before
	// deleting the seed PolicyTemplate itself. This ensures that bindings
	// whose user-cluster controller is no longer running (e.g. cluster
	// deleted) do not block the finalizer.
	if err := r.cleanupPolicyBindings(ctx, log, policyTemplate); err != nil {
		return fmt.Errorf("failed to cleanup PolicyBindings: %w", err)
	}

	err := r.seedClients.Each(ctx, log, func(_ string, seedClient ctrlruntimeclient.Client, log *zap.SugaredLogger) error {
		err := seedClient.Delete(ctx, &kubermaticv1.PolicyTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name: policyTemplate.Name,
			},
		})

		return ctrlruntimeclient.IgnoreNotFound(err)
	})
	if err != nil {
		return err
	}

	return kuberneteshelper.TryRemoveFinalizer(ctx, r.masterClient, policyTemplate, kubermaticv1.PolicyTemplateSeedCleanupFinalizer)
}

// cleanupPolicyBindings deletes all PolicyBindings that reference the given
// PolicyTemplate on every seed. For bindings in "cluster-*" namespaces where
// the owning Cluster no longer exists, the cleanup finalizer is force-removed
// first (the user-cluster controller that would normally handle it is gone).
func (r *reconciler) cleanupPolicyBindings(ctx context.Context, log *zap.SugaredLogger, policyTemplate *kubermaticv1.PolicyTemplate) error {
	return r.seedClients.Each(ctx, log, func(_ string, seedClient ctrlruntimeclient.Client, log *zap.SugaredLogger) error {
		bindingList := &kubermaticv1.PolicyBindingList{}
		if err := seedClient.List(ctx, bindingList); err != nil {
			return fmt.Errorf("failed to list PolicyBindings: %w", err)
		}

		for i := range bindingList.Items {
			binding := &bindingList.Items[i]
			if binding.Spec.PolicyTemplateRef.Name != policyTemplate.Name {
				continue
			}

			// If the binding lives in a cluster-* namespace and the Cluster
			// is gone, the user-cluster PolicyBinding controller is no longer
			// running and will never remove its finalizer. Force-remove it so
			// the binding can be garbage-collected.
			if strings.HasPrefix(binding.Namespace, "cluster-") &&
				kuberneteshelper.HasFinalizer(binding, kubermaticv1.PolicyBindingCleanupFinalizer) {
				clusterName := strings.TrimPrefix(binding.Namespace, "cluster-")
				cluster := &kubermaticv1.Cluster{}
				if err := seedClient.Get(ctx, ctrlruntimeclient.ObjectKey{Name: clusterName}, cluster); apierrors.IsNotFound(err) {
					log.Infow("Force-removing cleanup finalizer from orphaned PolicyBinding", "binding", binding.Name, "namespace", binding.Namespace)
					if err := kuberneteshelper.TryRemoveFinalizer(ctx, seedClient, binding, kubermaticv1.PolicyBindingCleanupFinalizer); err != nil {
						return fmt.Errorf("failed to remove finalizer from orphaned PolicyBinding %s/%s: %w", binding.Namespace, binding.Name, err)
					}
				} else if err != nil {
					return fmt.Errorf("failed to get Cluster %s: %w", clusterName, err)
				}
			}

			if err := seedClient.Delete(ctx, binding); ctrlruntimeclient.IgnoreNotFound(err) != nil {
				return fmt.Errorf("failed to delete PolicyBinding %s/%s: %w", binding.Namespace, binding.Name, err)
			}
		}

		// Re-list to verify all bindings referencing this template are fully gone.
		remainingList := &kubermaticv1.PolicyBindingList{}
		if err := seedClient.List(ctx, remainingList); err != nil {
			return fmt.Errorf("failed to re-list PolicyBindings: %w", err)
		}
		for _, binding := range remainingList.Items {
			if binding.Spec.PolicyTemplateRef.Name == policyTemplate.Name {
				return fmt.Errorf("PolicyBinding %s/%s still exists, will retry", binding.Namespace, binding.Name)
			}
		}

		return nil
	})
}

func policyTemplateReconcilerFactory(policyTemplate *kubermaticv1.PolicyTemplate) reconciling.NamedPolicyTemplateReconcilerFactory {
	return func() (string, reconciling.PolicyTemplateReconciler) {
		return policyTemplate.Name, func(pt *kubermaticv1.PolicyTemplate) (*kubermaticv1.PolicyTemplate, error) {
			pt.Labels = policyTemplate.Labels
			pt.Annotations = policyTemplate.Annotations
			pt.Spec = policyTemplate.Spec
			return pt, nil
		}
	}
}
