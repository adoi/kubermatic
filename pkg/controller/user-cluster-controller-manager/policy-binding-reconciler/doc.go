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

// Package policybindingreconciler implements the controller that manages PolicyBinding resources,
// translating them into appropriate Kyverno policies in the user cluster.
// It is responsible for creating, updating, and deleting Kyverno policies based on PolicyTemplate
// specifications referenced from PolicyBinding resources.
// It applies the Kyverno policies to the user cluster IF the PolicyBinding conditions are met.
package policybindingreconciler
