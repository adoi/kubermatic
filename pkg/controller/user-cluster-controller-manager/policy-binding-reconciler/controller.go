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

package policybindingreconciler

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	kyvernov1 "github.com/kyverno/kyverno/api/kyverno/v1"
	kubermaticv1 "k8c.io/kubermatic/sdk/v2/apis/kubermatic/v1"
	userclustercontrollermanager "k8c.io/kubermatic/v2/pkg/controller/user-cluster-controller-manager"
	kuberneteshelper "k8c.io/kubermatic/v2/pkg/kubernetes"
	"k8c.io/kubermatic/v2/pkg/resources/reconciling"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	ControllerName = "kkp-policy-binding-reconciler"
)

// PolicyBindingCleanupFinalizer is the name of the finalizer used to clean up Kyverno policies
const PolicyBindingCleanupFinalizer = "kubermatic.k8c.io/policy-binding-cleanup"

// Labels and annotations for Kyverno policy resources
// TODO: Move this
const (
	// LabelPolicyBindingName is the label that identifies which PolicyBinding created this Kyverno policy
	LabelPolicyBindingName = "policy.kubermatic.k8c.io/binding-name"

	// LabelPolicyTemplateName is the label that identifies which PolicyTemplate this Kyverno policy is based on
	LabelPolicyTemplateName = "policy.kubermatic.k8c.io/template-name"

	// AnnotationManagedBy is the annotation that identifies the controller that manages this resource
	AnnotationManagedBy = "policy.kubermatic.k8c.io/managed-by"
)

type reconciler struct {
	log             *zap.SugaredLogger
	recorder        record.EventRecorder
	client          ctrlruntimeclient.Client
	userClient      ctrlruntimeclient.Client
	clusterName     string
	clusterIsPaused userclustercontrollermanager.IsPausedChecker
}

func Add(
	mgr manager.Manager,
	userMgr manager.Manager,
	log *zap.SugaredLogger,
	clusterName string,
	clusterIsPaused userclustercontrollermanager.IsPausedChecker,
) error {
	r := &reconciler{
		log:             log.Named(ControllerName),
		recorder:        mgr.GetEventRecorderFor(ControllerName),
		client:          mgr.GetClient(),
		userClient:      userMgr.GetClient(),
		clusterName:     clusterName,
		clusterIsPaused: clusterIsPaused,
	}

	_, err := builder.ControllerManagedBy(mgr).
		Named(ControllerName).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
		}).
		For(&kubermaticv1.PolicyBinding{}).
		Build(r)

	return err
}

// TODO: Update Reason fields in conditions

// Reconcile processes PolicyBinding objects and ensures the corresponding Kyverno policies
// exist in the user cluster with the correct configuration.
func (r *reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := r.log.With("policybinding", request.Name)
	log.Debug("Processing")

	policyBinding := &kubermaticv1.PolicyBinding{}
	if err := r.client.Get(ctx, request.NamespacedName, policyBinding); err != nil {
		return reconcile.Result{}, ctrlruntimeclient.IgnoreNotFound(err)
	}

	err := r.reconcile(ctx, log, policyBinding)
	if err != nil {
		r.recorder.Event(policyBinding, corev1.EventTypeWarning, "ReconcilingError", err.Error())
	}

	return reconcile.Result{}, err
}

func (r *reconciler) reconcile(ctx context.Context, log *zap.SugaredLogger, policyBinding *kubermaticv1.PolicyBinding) error {
	// handling deletion
	if !policyBinding.DeletionTimestamp.IsZero() {
		if err := r.handleDeletion(ctx, log, policyBinding); err != nil {
			return fmt.Errorf("failed to handle deletion of policy binding: %w", err)
		}
		return nil
	}

	// add the cleanup finalizer if it doesn't exist
	if !kuberneteshelper.HasFinalizer(policyBinding, PolicyBindingCleanupFinalizer) {
		if err := kuberneteshelper.TryAddFinalizer(ctx, r.client, policyBinding, PolicyBindingCleanupFinalizer); err != nil {
			return fmt.Errorf("failed to add finalizer: %w", err)
		}
	}

	// get the referenced PolicyTemplate
	policyTemplate, err := r.getPolicyTemplate(ctx, policyBinding)
	if err != nil {
		return r.updateBindingStatus(ctx, policyBinding, false, metav1.Condition{
			Type:    string(kubermaticv1.PolicyBindingConditionTemplateValid),
			Status:  metav1.ConditionFalse,
			Reason:  "TemplateNotFound",
			Message: fmt.Sprintf("Failed to fetch referenced PolicyTemplate: %v", err),
		})
	}

	// Verify that this PolicyTemplate applies to the current cluster by checking the target selectors
	applicable, err := r.policyTemplateAppliesToCluster(ctx, policyTemplate)
	if err != nil {
		return r.updateBindingStatus(ctx, policyBinding, false, metav1.Condition{
			Type:    string(kubermaticv1.PolicyBindingConditionTemplateValid),
			Status:  metav1.ConditionFalse,
			Reason:  "TargetCheckFailed",
			Message: fmt.Sprintf("Failed to check if template applies to this cluster: %v", err),
		})
	}

	if !applicable {
		log.Infow("PolicyTemplate does not target this cluster, skipping",
			"templateName", policyTemplate.Name,
			"bindingName", policyBinding.Name)

		return r.updateBindingStatus(ctx, policyBinding, false, metav1.Condition{
			Type:    string(kubermaticv1.PolicyBindingConditionTemplateValid),
			Status:  metav1.ConditionFalse,
			Reason:  "TemplateNotApplicable",
			Message: "The referenced PolicyTemplate does not target this cluster",
		})
	}

	// update the TemplateEnforced status field based on the template
	enforced := policyTemplate.Spec.Enforced
	if policyBinding.Status.TemplateEnforced == nil || *policyBinding.Status.TemplateEnforced != enforced {
		templateEnforced := enforced // Create a copy to ensure we get the address of a local variable
		policyBinding.Status.TemplateEnforced = &templateEnforced
		if err := r.client.Status().Update(ctx, policyBinding); err != nil {
			return fmt.Errorf("failed to update policy binding status: %w", err)
		}
	}

	// determine if the policy should be active based on enforcement and enabled settings
	shouldBeActive := true
	if policyTemplate.Spec.Enforced {
		// Template is enforced, always active
		shouldBeActive = true
	} else if policyBinding.Spec.Enabled != nil {
		// Template is not enforced, use the enabled flag from binding
		shouldBeActive = *policyBinding.Spec.Enabled
	} else {
		// If neither enforced nor enabled is set, default to inactive
		shouldBeActive = false
	}

	// if the policy shouldn't be active, make sure it's removed and update status
	if !shouldBeActive {
		if err := r.removeKyvernoPolicies(ctx, log, policyBinding); err != nil {
			return r.updateBindingStatus(ctx, policyBinding, false, metav1.Condition{
				Type:    string(kubermaticv1.PolicyBindingConditionKyvernoPolicyApplied),
				Status:  metav1.ConditionFalse,
				Reason:  "RemovalFailed",
				Message: fmt.Sprintf("Failed to remove Kyverno policies: %v", err),
			})
		}

		return r.updateBindingStatus(ctx, policyBinding, false, metav1.Condition{
			Type:    string(kubermaticv1.PolicyBindingConditionKyvernoPolicyApplied),
			Status:  metav1.ConditionTrue,
			Reason:  "PolicyInactive",
			Message: "Policy is inactive, no Kyverno policies deployed",
		})
	}

	// If we're here, the policy should be active - create/update the Kyverno ClusterPolicy
	if err := r.ensureKyvernoPolicy(ctx, log, policyBinding, policyTemplate); err != nil {
		return r.updateBindingStatus(ctx, policyBinding, false, metav1.Condition{
			Type:    string(kubermaticv1.PolicyBindingConditionKyvernoPolicyApplied),
			Status:  metav1.ConditionFalse,
			Reason:  "KyvernoPolicyCreationFailed",
			Message: fmt.Sprintf("Failed to create/update Kyverno policy: %v", err),
		})
	}

	// After successfully creating/updating the Kyverno policies:
	return r.updateBindingStatus(ctx, policyBinding, true, metav1.Condition{
		Type:    string(kubermaticv1.PolicyBindingConditionKyvernoPolicyApplied),
		Status:  metav1.ConditionTrue,
		Reason:  "KyvernoPolicyApplied",
		Message: "Successfully applied Kyverno policies",
	})
}

// ensureKyvernoPolicy ensures that the appropriate Kyverno policy exists in the user cluster
func (r *reconciler) ensureKyvernoPolicy(ctx context.Context, log *zap.SugaredLogger, policyBinding *kubermaticv1.PolicyBinding, policyTemplate *kubermaticv1.PolicyTemplate) error {
	// TODO: Implement the namespaced policy creation
	// if policyBinding.Spec.NamespacedPolicy {
	// 	log.Infow("Namespaced policies not yet implemented, creating cluster-wide policy instead",
	// 		"bindingName", policyBinding.Name)
	// }

	// Get a policy name that identifies this binding
	policyName := r.generateKyvernoPolicyName(policyBinding)

	// Create the Kyverno ClusterPolicy using the reconciler factory pattern
	clusterPolicyReconcilers := []reconciling.NamedKyvernoClusterPolicyReconcilerFactory{
		r.kyvernoClusterPolicyReconcilerFactory(policyBinding, policyTemplate, policyName),
	}

	// Apply the ClusterPolicy to the user cluster
	if err := reconciling.ReconcileKyvernoClusterPolicys(ctx, clusterPolicyReconcilers, "", r.userClient); err != nil {
		return fmt.Errorf("failed to reconcile Kyverno ClusterPolicy: %w", err)
	}

	log.Infow("Successfully created/updated Kyverno ClusterPolicy",
		"policyName", policyName,
		"bindingName", policyBinding.Name,
		"templateName", policyTemplate.Name)

	return nil
}

// kyvernoClusterPolicyReconcilerFactory creates a factory for reconciling Kyverno ClusterPolicy objects
func (r *reconciler) kyvernoClusterPolicyReconcilerFactory(policyBinding *kubermaticv1.PolicyBinding, policyTemplate *kubermaticv1.PolicyTemplate, policyName string) reconciling.NamedKyvernoClusterPolicyReconcilerFactory {
	return func() (string, reconciling.KyvernoClusterPolicyReconciler) {
		return policyName, func(existingPolicy *kyvernov1.ClusterPolicy) (*kyvernov1.ClusterPolicy, error) {
			// If the policy is new, initialize it
			if existingPolicy.CreationTimestamp.IsZero() {
				existingPolicy = &kyvernov1.ClusterPolicy{
					ObjectMeta: metav1.ObjectMeta{
						Name: policyName,
					},
				}
			}

			// Set the appropriate labels and annotations
			if existingPolicy.Labels == nil {
				existingPolicy.Labels = map[string]string{}
			}
			existingPolicy.Labels[LabelPolicyBindingName] = policyBinding.Name
			existingPolicy.Labels[LabelPolicyTemplateName] = policyTemplate.Name

			if existingPolicy.Annotations == nil {
				existingPolicy.Annotations = map[string]string{}
			}
			existingPolicy.Annotations[AnnotationManagedBy] = ControllerName

			// TODO: Set the fields from the PolicyTemplate here
			// We have fields like title, description, etc. that we should set
			// There will also be fields like severity and so on that will be set as annotations

			// Convert Kubermatic PolicyTemplate to Kyverno ClusterPolicy
			policySpec, err := convertToKyvernoPolicySpec(policyTemplate)
			if err != nil {
				return nil, fmt.Errorf("failed to convert policy template to Kyverno policy: %w", err)
			}

			// Only update if the spec has changed to avoid unnecessary updates
			if !equality.Semantic.DeepEqual(existingPolicy.Spec, policySpec) {
				existingPolicy.Spec = policySpec
			}

			return existingPolicy, nil
		}
	}
}

// convertToKyvernoPolicySpec converts a Kubermatic PolicyTemplate to a Kyverno policy spec
func convertToKyvernoPolicySpec(policyTemplate *kubermaticv1.PolicyTemplate) (kyvernov1.Spec, error) {
	// If the PolicyTemplate has no PolicySpec, return the default spec
	if policyTemplate.Spec.PolicySpec.Raw == nil {
		return kyvernov1.Spec{}, nil
	}

	// Since PolicySpec is a RawExtension containing a Kyverno policy spec in JSON format,
	// we need to unmarshal it directly into the Kyverno Spec structure
	var kyvernoSpec kyvernov1.Spec
	if err := json.Unmarshal(policyTemplate.Spec.PolicySpec.Raw, &kyvernoSpec); err != nil {
		// If there's an error parsing, log it but use our default spec
		runtime.HandleError(fmt.Errorf("error unmarshaling Kyverno policy spec from PolicyTemplate: %v", err))
		return kyvernov1.Spec{}, err
	}

	// Ensure we always enforce certain fields regardless of what was in the template
	if kyvernoSpec.ValidationFailureAction == "" {
		kyvernoSpec.ValidationFailureAction = kyvernov1.Enforce
	}

	return kyvernoSpec, nil
}

// generateKyvernoPolicyName creates a unique name for the Kyverno policy based on the binding
func (r *reconciler) generateKyvernoPolicyName(policyBinding *kubermaticv1.PolicyBinding) string {
	return fmt.Sprintf("kb-%s", policyBinding.Name)
}

// policyTemplateAppliesToCluster checks if the given PolicyTemplate should be applied
func (r *reconciler) policyTemplateAppliesToCluster(ctx context.Context, policyTemplate *kubermaticv1.PolicyTemplate) (bool, error) {
	// Get the current cluster
	cluster := &kubermaticv1.Cluster{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: r.clusterName}, cluster); err != nil {
		return false, fmt.Errorf("failed to get cluster: %w", err)
	}

	// Get the project for this cluster
	projectID, exists := cluster.Labels[kubermaticv1.ProjectIDLabelKey]
	if !exists {
		return false, fmt.Errorf("cluster %s has no project ID label", r.clusterName)
	}

	project := &kubermaticv1.Project{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: projectID}, project); err != nil {
		return false, fmt.Errorf("failed to get project %s: %w", projectID, err)
	}

	switch policyTemplate.Spec.Visibility {
	case kubermaticv1.PolicyTemplateVisibilityGlobal:
		if policyTemplate.Spec.Target != nil && policyTemplate.Spec.Target.ProjectSelector != nil {
			selector, err := metav1.LabelSelectorAsSelector(policyTemplate.Spec.Target.ProjectSelector)
			if err != nil {
				return false, fmt.Errorf("invalid projectSelector in PolicyTemplate %s: %w", policyTemplate.Name, err)
			}

			if !selector.Matches(labels.Set(project.Labels)) {
				return false, nil
			}
		}

	case kubermaticv1.PolicyTemplateVisibilityProject:
		if policyTemplate.Spec.ProjectID == "" || policyTemplate.Spec.ProjectID != project.Name {
			return false, nil
		}

	default:
		return false, nil
	}

	// Check cluster selector if specified
	if policyTemplate.Spec.Target != nil && policyTemplate.Spec.Target.ClusterSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(policyTemplate.Spec.Target.ClusterSelector)
		if err != nil {
			return false, fmt.Errorf("invalid clusterSelector in PolicyTemplate %s: %w", policyTemplate.Name, err)
		}

		if !selector.Matches(labels.Set(cluster.Labels)) {
			return false, nil
		}
	}

	// All checks passed
	return true, nil
}

func (r *reconciler) getPolicyTemplate(ctx context.Context, policyBinding *kubermaticv1.PolicyBinding) (*kubermaticv1.PolicyTemplate, error) {
	policyTemplate := &kubermaticv1.PolicyTemplate{}
	err := r.client.Get(ctx, ctrlruntimeclient.ObjectKey{
		Name: policyBinding.Spec.PolicyTemplateRef.Name,
	}, policyTemplate)

	return policyTemplate, err
}

func (r *reconciler) handleDeletion(ctx context.Context, log *zap.SugaredLogger, policyBinding *kubermaticv1.PolicyBinding) error {
	if !kuberneteshelper.HasFinalizer(policyBinding, PolicyBindingCleanupFinalizer) {
		return nil
	}

	// Remove the Kyverno policies from the user cluster
	if err := r.removeKyvernoPolicies(ctx, log, policyBinding); err != nil {
		return err
	}

	// Remove the finalizer
	return kuberneteshelper.TryRemoveFinalizer(ctx, r.client, policyBinding, PolicyBindingCleanupFinalizer)
}

func (r *reconciler) removeKyvernoPolicies(ctx context.Context, log *zap.SugaredLogger, policyBinding *kubermaticv1.PolicyBinding) error {
	// Generate the policy name
	policyName := r.generateKyvernoPolicyName(policyBinding)

	// Delete the ClusterPolicy
	policy := &kyvernov1.ClusterPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: policyName,
		},
	}

	err := r.userClient.Delete(ctx, policy)
	// Ignore NotFound errors since the policy might not exist
	err = ctrlruntimeclient.IgnoreNotFound(err)
	if err != nil {
		return fmt.Errorf("failed to delete Kyverno ClusterPolicy %s: %w", policyName, err)
	}

	log.Infow("Successfully removed Kyverno ClusterPolicy", "policyName", policyName)
	return nil
}

// updateBindingStatus sets a condition on the PolicyBinding and updates its status
// using a pattern that properly handles retries and conflicts
func (r *reconciler) updateBindingStatus(ctx context.Context, policyBinding *kubermaticv1.PolicyBinding, active bool, condition metav1.Condition) error {
	return updatePolicyBindingStatus(ctx, r.client, policyBinding, func(binding *kubermaticv1.PolicyBinding) {
		// Set generation
		binding.Status.ObservedGeneration = binding.Generation

		// Set active status
		activeStatus := active
		binding.Status.Active = &activeStatus

		// Update the condition
		meta.SetStatusCondition(&binding.Status.Conditions, condition)

		// Also update the Ready condition based on other conditions
		readyCondition := calculateReadyCondition(binding)
		meta.SetStatusCondition(&binding.Status.Conditions, readyCondition)
	})
}

// calculateReadyCondition determines if the PolicyBinding is ready based on all conditions
func calculateReadyCondition(policyBinding *kubermaticv1.PolicyBinding) metav1.Condition {
	// A PolicyBinding is Ready when all other conditions are True
	for _, condition := range policyBinding.Status.Conditions {
		if condition.Status == metav1.ConditionFalse {
			return metav1.Condition{
				Type:    string(kubermaticv1.PolicyBindingConditionReady),
				Status:  metav1.ConditionFalse,
				Reason:  condition.Reason,
				Message: condition.Message,
			}
		}
	}

	return metav1.Condition{
		Type:    string(kubermaticv1.PolicyBindingConditionReady),
		Status:  metav1.ConditionTrue,
		Reason:  "AllConditionsTrue",
		Message: "All policy binding conditions are true",
	}
}

// updatePolicyBindingStatus is a helper that handles the retry logic when updating a PolicyBinding's status
func updatePolicyBindingStatus(ctx context.Context, client ctrlruntimeclient.Client, binding *kubermaticv1.PolicyBinding, modify func(*kubermaticv1.PolicyBinding)) error {
	// Get a fresh copy to avoid conflicts
	currentBinding := &kubermaticv1.PolicyBinding{}
	if err := client.Get(ctx, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, currentBinding); err != nil {
		return err
	}

	// Create a deep copy for comparison
	before := currentBinding.DeepCopy()

	// Apply the modifications to the fresh copy
	modify(currentBinding)

	// Only update if something changed
	if equality.Semantic.DeepEqual(before.Status, currentBinding.Status) {
		return nil
	}

	// Update the status in the API
	if err := client.Status().Update(ctx, currentBinding); err != nil {
		return fmt.Errorf("failed to update policy binding status: %w", err)
	}

	return nil
}
