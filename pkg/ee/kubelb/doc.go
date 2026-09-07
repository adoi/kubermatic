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

/*
Package kubelbcontroller contains a controller that is responsible for configuring and installing KubeLB CCM for user clusters.
It is responsible for the following:
1. KubeLB cluster: register the user cluster as a tenant.
2. Seed cluster: deploy KubeLB CCM to the user cluster namespace.
3. User cluster: configure RBAC for the KubeLB CCM.

Cleanup persists its progress on the Cluster and finishes even if KubeLB is
re-enabled during teardown. WAF policies are drained while the CCM still runs.
The CCM is then scaled to zero, and KKP waits for its Pods to stop before
removing the tenant proxy and releasing the proxy's TenantState finalizer with
the scoped tenant credential. This prevents resource recreation without
depending on credentials that the management Tenant controller revokes during
deletion. User resources are detached only after the management Tenant and its
namespace have disappeared; user-cluster RBAC/CRDs and seed resources are
removed last. Missing optional APIs are tolerated, while other errors and
foreign finalizers keep cleanup pending. A pending or failing cleanup reports
what it is waiting on as events on the Cluster and in the controller log.
*/
package kubelbcontroller
