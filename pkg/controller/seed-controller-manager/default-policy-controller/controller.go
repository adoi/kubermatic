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

package defaultpolicycontroller

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"k8c.io/kubermatic/v2/pkg/version/kubermatic"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/builder"

	kubermaticv1 "k8c.io/kubermatic/sdk/v2/apis/kubermatic/v1"
	clusterclient "k8c.io/kubermatic/v2/pkg/cluster/client"
	"k8c.io/kubermatic/v2/pkg/controller/util"
	"k8c.io/kubermatic/v2/pkg/provider"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	ControllerName = "kkp-default-policy-controller"

	// defaultPolicyBindingPrefix is the prefix for managed policy bindings
	defaultPolicyBindingPrefix = "default-"

	// Annotation keys
	annotationPolicyManagedBy = "policy.kubermatic.k8c.io/managed-by"
	labelPolicyTemplateName   = "policy.kubermatic.k8c.io/template-name"
)

// UserClusterClientProvider provides functionality to get a user cluster client.
type UserClusterClientProvider interface {
	GetClient(ctx context.Context, c *kubermaticv1.Cluster, options ...clusterclient.ConfigOption) (ctrlruntimeclient.Client, error)
}

type Reconciler struct {
	ctrlruntimeclient.Client

	workerName                    string
	recorder                      record.EventRecorder
	seedGetter                    provider.SeedGetter
	configGetter                  provider.KubermaticConfigurationGetter
	userClusterConnectionProvider UserClusterClientProvider
	log                           *zap.SugaredLogger
	versions                      kubermatic.Versions
}

func Add(ctx context.Context, mgr manager.Manager, numWorkers int, workerName string, seedGetter provider.SeedGetter, kubermaticConfigurationGetter provider.KubermaticConfigurationGetter, userClusterConnectionProvider UserClusterClientProvider, log *zap.SugaredLogger, versions kubermatic.Versions) error {
	reconciler := &Reconciler{
		Client: mgr.GetClient(),

		workerName:                    workerName,
		recorder:                      mgr.GetEventRecorderFor(ControllerName),
		seedGetter:                    seedGetter,
		configGetter:                  kubermaticConfigurationGetter,
		userClusterConnectionProvider: userClusterConnectionProvider,
		log:                           log,
		versions:                      versions,
	}

	_, err := builder.ControllerManagedBy(mgr).
		Named(ControllerName).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: numWorkers,
		}).
		// Watch for clusters
		For(&kubermaticv1.PolicyTemplate{}).
		// Watch changes for PolicyTemplates that have been enforced.
		Watches(&kubermaticv1.PolicyTemplate{}, enqueueClusters(reconciler, log), builder.WithPredicates(withEventFilter())).
		Build(reconciler)

	return err
}

func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	cluster := &kubermaticv1.Cluster{}
	if err := r.Get(ctx, request.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if cluster.DeletionTimestamp != nil {
		r.log.Debugw("Cluster is queued for deletion; no action required", "cluster", cluster.Name)
		return reconcile.Result{}, nil
	}

	result, err := util.ClusterReconcileWrapper(
		ctx,
		r,
		r.workerName,
		cluster,
		r.versions,
		kubermaticv1.ClusterConditionDefaultPolicyControllerReconcilingSuccess,
		func() (*reconcile.Result, error) {
			return r.reconcile(ctx, cluster)
		},
	)

	if result == nil || err != nil {
		result = &reconcile.Result{}
	}

	if err != nil {
		r.recorder.Event(cluster, corev1.EventTypeWarning, "ReconcilingError", err.Error())
	}

	return *result, err
}

func (r *Reconciler) reconcile(ctx context.Context, cluster *kubermaticv1.Cluster) (*reconcile.Result, error) {
	// If cluster is not healthy yet there is nothing to do.
	// If it gets healthy we'll get notified by the event. No need to requeue.
	if !cluster.Status.ExtendedHealth.AllHealthy() {
		return nil, nil
	}

	userClusterClient, err := r.userClusterConnectionProvider.GetClient(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get user cluster client: %w", err)
	}

	// Get the project for this cluster
	projectID, exists := cluster.Labels[kubermaticv1.ProjectIDLabelKey]
	if !exists {
		return nil, fmt.Errorf("cluster %s has no project ID label", cluster.Name)
	}

	project := &kubermaticv1.Project{}
	if err := r.Get(ctx, types.NamespacedName{Name: projectID}, project); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Debugw("Project not found, returning", "project", projectID)
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get project %s: %w", projectID, err)
	}

	// List all PolicyTemplates
	policyTemplatesList := &kubermaticv1.PolicyTemplateList{}
	if err := r.List(ctx, policyTemplatesList); err != nil {
		return nil, fmt.Errorf("failed to list PolicyTemplates: %w", err)
	}

	// Collect all policy templates that need to be installed/updated
	policyTemplates := []kubermaticv1.PolicyTemplate{}
	for _, policyTemplate := range policyTemplatesList.Items {
		if policyTemplate.DeletionTimestamp != nil {
			continue
		}

		// Check if the PolicyTemplate should be applied to this cluster
		if r.shouldApplyTemplateToCluster(&policyTemplate, cluster, project) {
			policyTemplates = append(policyTemplates, policyTemplate)
		}
	}

	// We don't want to fail the reconciliation if one policy template fails so we collect all the errors and return them as a single error.
	var errors []error
	for _, policyTemplate := range policyTemplates {
		err := r.ensurePolicyBinding(ctx, userClusterClient, policyTemplate, cluster)
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) == 0 && !cluster.Status.HasConditionValue(kubermaticv1.ClusterConditionDefaultPolicyControllerReconcilingSuccess, corev1.ConditionTrue) {
		if err := util.UpdateClusterStatus(ctx, r, cluster, func(c *kubermaticv1.Cluster) {
			util.SetClusterCondition(
				cluster,
				r.versions,
				kubermaticv1.ClusterConditionDefaultPolicyControllerReconcilingSuccess,
				corev1.ConditionTrue,
				"",
				"",
			)
		}); err != nil {
			return &reconcile.Result{}, err
		}
	}

	return nil, kerrors.NewAggregate(errors)
}

// shouldApplyTemplateToCluster determines if a PolicyTemplate should be applied to a given Cluster.
func (r *Reconciler) shouldApplyTemplateToCluster(template *kubermaticv1.PolicyTemplate, cluster *kubermaticv1.Cluster, project *kubermaticv1.Project) bool {
	// Templates must be either enforced or default to be considered
	if !template.Spec.Enforced && !template.Spec.Default {
		return false
	}

	// Check visibility and project matching
	switch template.Spec.Visibility {
	case kubermaticv1.PolicyTemplateVisibilityGlobal:
		// Global templates apply to all projects unless filtered by projectSelector
		if template.Spec.Target != nil && template.Spec.Target.ProjectSelector != nil {
			selector, err := metav1.LabelSelectorAsSelector(template.Spec.Target.ProjectSelector)
			if err != nil {
				r.log.Errorw("Invalid projectSelector in PolicyTemplate", "template", template.Name, "error", err)
				return false
			}

			if !selector.Matches(labels.Set(project.Labels)) {
				return false
			}
		}
		// If project selector matched or wasn't specified, continue to cluster selector check

	case kubermaticv1.PolicyTemplateVisibilityProject:
		// For project visibility, the projectID must match
		if template.Spec.ProjectID == "" || template.Spec.ProjectID != project.Name {
			return false
		}
		// If project matched, continue to cluster selector check

	case kubermaticv1.PolicyTemplateVisibilityCluster:
		// For cluster visibility, the projectID must also match
		if template.Spec.ProjectID == "" || template.Spec.ProjectID != project.Name {
			return false
		}

	default:
		// Unknown visibility type
		return false
	}

	// Check cluster selector if specified
	if template.Spec.Target != nil && template.Spec.Target.ClusterSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(template.Spec.Target.ClusterSelector)
		if err != nil {
			r.log.Errorw("Invalid clusterSelector in PolicyTemplate", "template", template.Name, "error", err)
			return false
		}

		if !selector.Matches(labels.Set(cluster.Labels)) {
			return false
		}
	}

	// All checks passed
	return true
}

// ensurePolicyBinding creates or updates a PolicyBinding for a template
func (r *Reconciler) ensurePolicyBinding(ctx context.Context, userClusterClient ctrlruntimeclient.Client,
	template kubermaticv1.PolicyTemplate, cluster *kubermaticv1.Cluster) error {

	// Determine binding name (using a consistent prefix)
	bindingName := fmt.Sprintf("%s%s", defaultPolicyBindingPrefix, template.Name)
	bindingNamespace := fmt.Sprintf("kube-system") // TODO: this will change talk about this, we need to decide on best approach

	// Check if binding already exists
	existingBinding := &kubermaticv1.PolicyBinding{}
	err := r.Get(ctx, types.NamespacedName{Name: bindingName, Namespace: bindingNamespace}, existingBinding)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check existing binding: %w", err)
	}

	// If binding doesn't exist, create it
	if apierrors.IsNotFound(err) {
		binding := &kubermaticv1.PolicyBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bindingName,
				Namespace: bindingNamespace,
				Labels: map[string]string{
					labelPolicyTemplateName: template.Name,
				},
				Annotations: map[string]string{
					annotationPolicyManagedBy: ControllerName,
				},
				// TODO: add owner reference
			},
			Spec: kubermaticv1.PolicyBindingSpec{
				PolicyTemplateRef: corev1.ObjectReference{
					Name: template.Name,
				},
				Enabled: boolPtr(true),
			},
		}

		// Mark as enforced if applicable
		if template.Spec.Enforced {
			binding.Annotations[kubermaticv1.AnnotationPolicyEnforced] = "true"
		}

		if template.Spec.Default {
			binding.Annotations[kubermaticv1.AnnotationPolicyDefault] = "true"
		}

		if err := r.Create(ctx, binding); err != nil {
			return fmt.Errorf("failed to create policy binding: %w", err)
		}

		r.recorder.Eventf(cluster, corev1.EventTypeNormal, "PolicyBindingCreated",
			"Created default policy binding %s for template %s", bindingName, template.Name)
		return nil
	}

	// Update the existing binding if needed
	updated := false

	// Update labels if needed
	if existingBinding.Labels == nil {
		existingBinding.Labels = map[string]string{}
	}
	if existingBinding.Labels[labelPolicyTemplateName] != template.Name {
		existingBinding.Labels[labelPolicyTemplateName] = template.Name
		updated = true
	}

	// Update annotations if needed
	if existingBinding.Annotations == nil {
		existingBinding.Annotations = map[string]string{}
	}
	if existingBinding.Annotations[annotationPolicyManagedBy] != ControllerName {
		existingBinding.Annotations[annotationPolicyManagedBy] = ControllerName
		updated = true
	}

	// Update enforcement annotation based on template
	hasEnforced := existingBinding.Annotations[kubermaticv1.AnnotationPolicyEnforced] == "true"
	if template.Spec.Enforced && !hasEnforced {
		existingBinding.Annotations[kubermaticv1.AnnotationPolicyEnforced] = "true"
		updated = true
	} else if !template.Spec.Enforced && hasEnforced {
		delete(existingBinding.Annotations, kubermaticv1.AnnotationPolicyEnforced)
		updated = true
	}

	// Update template reference if needed
	if existingBinding.Spec.PolicyTemplateRef.Name != template.Name {
		existingBinding.Spec.PolicyTemplateRef.Name = template.Name
		updated = true
	}

	// If any updates are needed, apply them
	// This is not a good approach, we need to use the reconciler factory pattern
	if updated {
		if err := r.Update(ctx, existingBinding); err != nil {
			return fmt.Errorf("failed to update policy binding: %w", err)
		}
		r.recorder.Eventf(cluster, corev1.EventTypeNormal, "PolicyBindingUpdated",
			"Updated policy binding %s for template %s", bindingName, template.Name)
	}

	return nil
}

func enqueueClusters(client ctrlruntimeclient.Client, log *zap.SugaredLogger) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, a ctrlruntimeclient.Object) []reconcile.Request {
		policyTemplate, ok := a.(*kubermaticv1.PolicyTemplate)
		if !ok {
			return nil
		}

		// Only process templates that are enforced or default
		if !policyTemplate.Spec.Enforced && !policyTemplate.Spec.Default {
			return nil
		}

		// List all clusters
		clusterList := &kubermaticv1.ClusterList{}
		if err := client.List(ctx, clusterList); err != nil {
			log.Errorw("Failed to list clusters", "error", err)
			utilruntime.HandleError(fmt.Errorf("failed to list clusters: %w", err))
			return nil
		}

		// Get all projects for project selector filtering
		projectList := &kubermaticv1.ProjectList{}
		if err := client.List(ctx, projectList); err != nil {
			log.Errorw("Failed to list projects", "error", err)
			utilruntime.HandleError(fmt.Errorf("failed to list projects: %w", err))
			return nil
		}

		// Organize projects by ID for quick lookup
		projectsByID := make(map[string]*kubermaticv1.Project)
		for i := range projectList.Items {
			project := &projectList.Items[i]
			projectsByID[project.Name] = project
		}

		var requests []reconcile.Request
		for i := range clusterList.Items {
			cluster := &clusterList.Items[i]

			// Skip clusters that are being deleted
			if cluster.DeletionTimestamp != nil {
				continue
			}

			// Get the project for the cluster
			projectID, exists := cluster.Labels[kubermaticv1.ProjectIDLabelKey]
			if !exists {
				log.Debugw("Cluster has no project ID label, skipping", "cluster", cluster.Name)
				continue
			}

			project, exists := projectsByID[projectID]
			if !exists {
				log.Debugw("Project not found for cluster, skipping", "cluster", cluster.Name, "projectID", projectID)
				continue
			}

			// Check if the policy template should apply to this cluster
			// We need to create a temp reconciler just to call shouldApplyTemplateToCluster
			tempReconciler := &Reconciler{
				Client: client,
				log:    log,
			}
			if tempReconciler.shouldApplyTemplateToCluster(policyTemplate, cluster, project) {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name: cluster.Name,
					},
				})
			}
		}

		return requests
	})
}

func withEventFilter() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			obj := e.Object.(*kubermaticv1.PolicyTemplate)
			if obj.GetDeletionTimestamp() != nil {
				return false
			}

			// Consider both enforced and default policies
			return obj.Spec.Enforced || obj.Spec.Default
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj := e.ObjectOld.(*kubermaticv1.PolicyTemplate)
			newObj := e.ObjectNew.(*kubermaticv1.PolicyTemplate)

			if newObj.GetDeletionTimestamp() != nil {
				return false
			}

			// Check if enforced or default status changed
			if (newObj.Spec.Enforced != oldObj.Spec.Enforced) || (newObj.Spec.Default != oldObj.Spec.Default) {
				return newObj.Spec.Enforced || newObj.Spec.Default
			}

			// If it's still enforced or default, check if there's a generation change
			if newObj.Spec.Enforced || newObj.Spec.Default {
				return newObj.GetGeneration() != oldObj.GetGeneration()
			}
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			obj := e.Object.(*kubermaticv1.PolicyTemplate)
			if obj.GetDeletionTimestamp() != nil {
				return false
			}

			// Consider both enforced and default policies
			return obj.Spec.Enforced || obj.Spec.Default
		},
	}
}

// Helper functions
func boolPtr(b bool) *bool {
	return &b
}
