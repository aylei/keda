package activate

import (
	"context"
	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sync"
)

const (
	ActivateAnno = "keda.sh/activate"
)

var DefaultRegistry *Registry

func init() {
	DefaultRegistry = newRegistry()
}

type Registry struct {
	m map[string]chan<- bool
	sync.RWMutex
}

func newRegistry() *Registry {
	return &Registry{
		m: map[string]chan<- bool{},
	}
}

func (r *Registry) Regist(key string, ch chan<- bool) {
	r.Lock()
	defer r.Unlock()
	r.m[key] = ch
}

func (r *Registry) get(key string) (chan<- bool, bool) {
	r.RLock()
	defer r.RUnlock()
	ch, ok := r.m[key]
	return ch, ok
}

type ActivationController struct {
	registry *Registry
	kubeCli  client.Client
}

func New(r *Registry) *ActivationController {
	return &ActivationController{registry: r}
}

func (r *ActivationController) SetupWithManager(mgr ctrl.Manager) error {
	r.kubeCli = mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		For(&kedav1alpha1.ScaledObject{}, builder.WithPredicates(predicate.AnnotationChangedPredicate{})).Complete(r)
}

func (r *ActivationController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLogger := log.FromContext(ctx)
	scaledObject := &kedav1alpha1.ScaledObject{}
	err := r.kubeCli.Get(ctx, req.NamespacedName, scaledObject)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		reqLogger.Error(err, "failed to get ScaledObject")
		return ctrl.Result{}, err
	}

	if _, ok := scaledObject.Annotations[ActivateAnno]; ok {
		reqLogger.Info("activate scaledObject", "name", scaledObject.Name)
		ch, ok := r.registry.get(scaledObject.GenerateIdentifier())
		if ok {
			reqLogger.Info("activate scaled object", "namespace", scaledObject.Namespace, "name", scaledObject.Name)
			ch <- true
		}
	}
	old := scaledObject.DeepCopy()
	delete(scaledObject.Annotations, ActivateAnno)
	if equality.Semantic.DeepEqual(old.Annotations, scaledObject.Annotations) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.kubeCli.Patch(ctx, scaledObject, client.MergeFrom(old))
}
