package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/anthropp/ratelimiter/internal/config"
)

const (
	coordinatorName = "coordinator"
	workerName      = "ratelim-workers"
	configMapName   = "ratelim-config"
	workerPort      = 8080
	coordinatorPort = 8081
)

// Kube owns the rate limiter's lifecycle on the cluster: the loadgen creates,
// scales, and kills the coordinator and workers (design D5).
type Kube struct {
	cs    *kubernetes.Clientset
	ns    string
	image string
}

func NewKube() (*Kube, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		// Local development fallback.
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("no in-cluster or kubeconfig credentials: %w", err)
		}
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		ns = "ratelimiter"
	}
	image := os.Getenv("IMAGE")
	if image == "" {
		return nil, fmt.Errorf("IMAGE env var must name the ratelim container image")
	}
	return &Kube{cs: cs, ns: ns, image: image}, nil
}

// LoadConfig reads the coordinator's ConfigMap so scenario assertions use the
// same tenant limits the coordinator enforces (single source of truth).
func (k *Kube) LoadConfig(ctx context.Context) (*config.Config, error) {
	cm, err := k.cs.CoreV1().ConfigMaps(k.ns).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp("", "ratelim-config-*.yaml")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(cm.Data["config.yaml"]); err != nil {
		return nil, err
	}
	tmp.Close()
	return config.Load(tmp.Name())
}

func int32p(v int32) *int32 { return &v }

func (k *Kube) coordinatorDeployment() *appsv1.Deployment {
	labels := map[string]string{"app": "ratelim", "role": "coordinator"}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: coordinatorName, Namespace: k.ns, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32p(1),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "coordinator",
						Image: k.image,
						Args:  []string{"coordinator", "--config", "/etc/ratelim/config.yaml", "--addr", fmt.Sprintf(":%d", coordinatorPort)},
						Ports: []corev1.ContainerPort{{ContainerPort: coordinatorPort}},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "config", MountPath: "/etc/ratelim", ReadOnly: true,
						}},
						Resources: corev1.ResourceRequirements{
							// Requests stay tiny because GKE system pods leave
							// only ~200m schedulable per e2-medium node; limits
							// do the real capping (Burstable QoS).
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstrFromInt(coordinatorPort)}},
							PeriodSeconds: 2,
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "config",
						VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
						}},
					}},
				},
			},
		},
	}
}

// Worker CPU is per-scenario: the scaling scenario caps workers at a tiny
// 50m so a single loadgen on a 2-vCPU node can saturate even 4 replicas
// while staying inside its own clean generation range (~2000 rps); every
// other scenario uses 400m so worker CPU exhaustion (a separate phenomenon,
// see future work F2) does not contaminate what the scenario demonstrates.
const (
	workerCPUDefault = "400m"
	workerCPUScaling = "50m"
)

func (k *Kube) workerDeployment(replicas int32, cpu string) *appsv1.Deployment {
	labels := map[string]string{"app": "ratelim", "role": "worker"}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: k.ns, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32p(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			// Recreate: nodes are small (940m allocatable), so spec changes
			// (e.g. the per-scenario CPU cap) must not require surge room for
			// old and new pods at once. Losing leased tokens between
			// scenarios is fine by design.
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "worker",
						Image: k.image,
						Args:  []string{"worker", "--coordinator", fmt.Sprintf("http://%s:%d", coordinatorName, coordinatorPort), "--addr", fmt.Sprintf(":%d", workerPort)},
						Ports: []corev1.ContainerPort{{ContainerPort: workerPort}},
						Env:   []corev1.EnvVar{{Name: "GOMAXPROCS", Value: "1"}},
						Resources: corev1.ResourceRequirements{
							// Request small (scheduling headroom on e2-medium
							// nodes is ~200m, and it must not exceed the
							// smallest per-scenario limit); the limit is what
							// caps worker CPU per scenario.
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse("128Mi")},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstrFromInt(workerPort)}},
							PeriodSeconds: 2,
						},
					}},
				},
			},
		},
	}
}

func (k *Kube) service(name string, selectorRole string, port int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.ns},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "ratelim", "role": selectorRole},
			Ports:    []corev1.ServicePort{{Port: port, TargetPort: intstrFromInt(port)}},
		},
	}
}

func (k *Kube) applyDeployment(ctx context.Context, d *appsv1.Deployment) error {
	existing, err := k.cs.AppsV1().Deployments(k.ns).Get(ctx, d.Name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		_, err = k.cs.AppsV1().Deployments(k.ns).Create(ctx, d, metav1.CreateOptions{})
		return err
	} else if err != nil {
		return err
	}
	existing.Spec = d.Spec
	_, err = k.cs.AppsV1().Deployments(k.ns).Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (k *Kube) applyService(ctx context.Context, s *corev1.Service) error {
	_, err := k.cs.CoreV1().Services(k.ns).Create(ctx, s, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil // Service spec is static; no need to update.
	}
	return err
}

// EnsureRateLimiter creates or updates the coordinator and worker deployments
// and services, then waits until both are serving.
func (k *Kube) EnsureRateLimiter(ctx context.Context, workers int32, workerCPU string) error {
	if workerCPU == "" {
		workerCPU = workerCPUDefault
	}
	if err := k.applyDeployment(ctx, k.coordinatorDeployment()); err != nil {
		return fmt.Errorf("apply coordinator: %w", err)
	}
	if err := k.applyService(ctx, k.service(coordinatorName, "coordinator", coordinatorPort)); err != nil {
		return fmt.Errorf("apply coordinator service: %w", err)
	}
	if err := k.applyDeployment(ctx, k.workerDeployment(workers, workerCPU)); err != nil {
		return fmt.Errorf("apply workers: %w", err)
	}
	if err := k.applyService(ctx, k.service(workerName, "worker", workerPort)); err != nil {
		return fmt.Errorf("apply worker service: %w", err)
	}
	if err := k.WaitDeploymentReady(ctx, coordinatorName, 1, 120*time.Second); err != nil {
		return err
	}
	if err := k.WaitDeploymentReady(ctx, workerName, workers, 120*time.Second); err != nil {
		return err
	}
	return k.waitHTTPReady(ctx, fmt.Sprintf("http://%s:%d/healthz", workerName, workerPort), 30*time.Second)
}

func (k *Kube) Scale(ctx context.Context, deployment string, replicas int32) error {
	patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	_, err := k.cs.AppsV1().Deployments(k.ns).Patch(ctx, deployment, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

func (k *Kube) WaitDeploymentReady(ctx context.Context, name string, replicas int32, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		d, err := k.cs.AppsV1().Deployments(k.ns).Get(ctx, name, metav1.GetOptions{})
		if err == nil && d.Status.ReadyReplicas == replicas && d.Status.UpdatedReplicas == replicas &&
			d.Status.Replicas == replicas && d.Generation == d.Status.ObservedGeneration {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("deployment %s not ready with %d replicas after %v", name, replicas, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (k *Kube) waitHTTPReady(ctx context.Context, url string, timeout time.Duration) error {
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("%s not serving after %v", url, timeout)
}

func (k *Kube) pods(ctx context.Context, role string) ([]corev1.Pod, error) {
	list, err := k.cs.CoreV1().Pods(k.ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=ratelim,role=" + role,
	})
	if err != nil {
		return nil, err
	}
	var running []corev1.Pod
	for _, p := range list.Items {
		if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning {
			running = append(running, p)
		}
	}
	sort.Slice(running, func(i, j int) bool { return running[i].Name < running[j].Name })
	return running, nil
}

// WorkerDecisions returns, per running worker pod, the total number of
// admission decisions (admitted + rejected) it has served, by scraping each
// pod's /v1/stats directly. Used to show load balance across replicas.
func (k *Kube) WorkerDecisions(ctx context.Context) (map[string]int64, error) {
	pods, err := k.pods(ctx, "worker")
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	out := make(map[string]int64, len(pods))
	for _, p := range pods {
		resp, err := client.Get(fmt.Sprintf("http://%s:%d/v1/stats", p.Status.PodIP, workerPort))
		if err != nil {
			continue
		}
		var body struct {
			Tenants map[string]map[string]int64 `json:"tenants"`
		}
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		var n int64
		for _, t := range body.Tenants {
			n += t["admitted"] + t["rejected"]
		}
		out[p.Name] = n
	}
	return out, nil
}

// WaitAllWorkersReachable probes the worker Service with keep-alive disabled
// (every probe opens a fresh connection, re-rolling the DNAT choice) until
// every running worker pod has served at least one probe. This is the only
// reliable signal that kube-proxy has programmed the new endpoints on this
// node — GKE's iptables minSyncPeriod can lag pod readiness by ~10s.
func (k *Kube) WaitAllWorkersReachable(ctx context.Context, workerURL string, timeout time.Duration) error {
	client := &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	base, err := k.WorkerDecisions(ctx)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for {
		pods, err := k.pods(ctx, "worker")
		if err != nil {
			return err
		}
		// Coupon-collector with margin: enough fresh connections that every
		// programmed endpoint is hit with overwhelming probability.
		// Probe a real tenant: only real admission checks increment the
		// per-worker decision counters the loop below reads.
		for i := 0; i < 12*len(pods); i++ {
			resp, err := client.Get(workerURL + "/v1/check/tenant-hi")
			if err == nil {
				resp.Body.Close()
			}
		}
		cur, err := k.WorkerDecisions(ctx)
		if err == nil {
			all := true
			for _, p := range pods {
				if cur[p.Name] <= base[p.Name] {
					all = false
					break
				}
			}
			if all {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not all worker pods receiving service traffic after %v (kube-proxy endpoint propagation)", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// KillOneWorkerNoReplacement abruptly kills one worker pod and simultaneously
// lowers desired replicas so the ReplicaSet does not create a replacement:
// the victim is marked cheapest-to-delete, the deployment is scaled down (the
// controller picks the victim), and the victim is then force-deleted so it
// dies abruptly instead of draining. Returns the victim pod name.
func (k *Kube) KillOneWorkerNoReplacement(ctx context.Context) (string, error) {
	pods, err := k.pods(ctx, "worker")
	if err != nil {
		return "", err
	}
	if len(pods) < 2 {
		return "", fmt.Errorf("need >=2 running workers, have %d", len(pods))
	}
	victim := pods[0].Name
	costPatch := []byte(`{"metadata":{"annotations":{"controller.kubernetes.io/pod-deletion-cost":"-999"}}}`)
	if _, err := k.cs.CoreV1().Pods(k.ns).Patch(ctx, victim, types.MergePatchType, costPatch, metav1.PatchOptions{}); err != nil {
		return "", err
	}
	if err := k.Scale(ctx, workerName, int32(len(pods)-1)); err != nil {
		return "", err
	}
	var zero int64
	err = k.cs.CoreV1().Pods(k.ns).Delete(ctx, victim, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	return victim, err
}

// KillCoordinator scales the coordinator to zero and force-deletes its pod so
// it dies abruptly.
func (k *Kube) KillCoordinator(ctx context.Context) error {
	if err := k.Scale(ctx, coordinatorName, 0); err != nil {
		return err
	}
	pods, err := k.pods(ctx, "coordinator")
	if err != nil {
		return err
	}
	var zero int64
	for _, p := range pods {
		if err := k.cs.CoreV1().Pods(k.ns).Delete(ctx, p.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// RestoreCoordinator scales the coordinator back up and waits for it to serve.
func (k *Kube) RestoreCoordinator(ctx context.Context) error {
	if err := k.Scale(ctx, coordinatorName, 1); err != nil {
		return err
	}
	return k.WaitDeploymentReady(ctx, coordinatorName, 1, 120*time.Second)
}
