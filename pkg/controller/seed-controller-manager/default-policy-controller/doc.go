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

/*
Package defaultpolicycontroller contains a controller that is responsible for installing default/enforced
policy templates for a cluster. It watches cluster resource and creates PolicyBinding resources for the default/enforced
policy templates. Some features of the controller are:
1. Enforced policy templates are always installed and cannot be overridden by the user.
2. If an enforced PolicyTemplate is updated, the controller will update all the existing PolicyBinding resources. For this functionality, the
controller watches for PolicyTemplate resources as well and reconciles the affected Clusters against them.
*/
package defaultpolicycontroller
