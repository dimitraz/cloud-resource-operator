package blobstorage

import (
	"context"
	"fmt"
	"time"

	"github.com/integr8ly/cloud-resource-operator/pkg/providers/aws"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	controllerruntime "sigs.k8s.io/controller-runtime"

	"github.com/integr8ly/cloud-resource-operator/pkg/providers"

	integreatlyv1alpha1 "github.com/integr8ly/cloud-resource-operator/pkg/apis/integreatly/v1alpha1"
	errorUtil "github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_blobstorage")

// Add creates a new BlobStorage Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileBlobStorage{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("blobstorage-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource BlobStorage
	err = c.Watch(&source.Kind{Type: &integreatlyv1alpha1.BlobStorage{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileBlobStorage implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileBlobStorage{}

// ReconcileBlobStorage reconciles a BlobStorage object
type ReconcileBlobStorage struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

func (r *ReconcileBlobStorage) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	providerList := []providers.BlobStorageProvider{aws.NewAWSBlobStorageProvider(r.client)}

	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling BlobStorage")
	ctx := context.TODO()
	cfgMgr := providers.NewConfigManager(providers.DefaultProviderConfigMapName, providers.DefaultConfigNamespace, r.client)

	// Fetch the BlobStorage instance
	instance := &integreatlyv1alpha1.BlobStorage{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	stratMap, err := cfgMgr.GetStrategyMappingForDeploymentType(ctx, instance.Spec.Type)
	if err != nil {
		return reconcile.Result{}, errorUtil.Wrapf(err, "failed to read deployment type config for deployment %s", instance.Spec.Type)
	}

	for _, p := range providerList {
		if p.SupportsStrategy(stratMap.BlobStorage) {
			if instance.GetDeletionTimestamp() != nil {
				if err := p.DeleteStorage(ctx, r.client, instance); err != nil {
					return reconcile.Result{}, errorUtil.Wrapf(err, "failed to perform provider-specific storage deletion")
				}
				return reconcile.Result{}, nil
			}

			bsi, err := p.CreateStorage(ctx, instance)
			if err != nil {
				return reconcile.Result{}, err
			}
			if bsi == nil {
				return reconcile.Result{}, errorUtil.New("secret data is still reconciling")
			}

			// create the secret with user aws credentials
			sec := &corev1.Secret{
				ObjectMeta: controllerruntime.ObjectMeta{
					Name:      instance.Spec.SecretRef.Name,
					Namespace: instance.Namespace,
				},
			}
			controllerruntime.CreateOrUpdate(ctx, r.client, sec, func(existing runtime.Object) error {
				e := existing.(*corev1.Secret)
				if err = controllerutil.SetControllerReference(instance, e, r.scheme); err != nil {
					return errorUtil.Wrapf(err, "failed to set owner on secret %s", sec.Name)
				}
				e.Data = bsi.DeploymentDetails.Data()
				e.Type = corev1.SecretTypeOpaque
				return nil
			})
			instance.Status.SecretRef = instance.Spec.SecretRef
			instance.Status.Strategy = stratMap.BlobStorage
			instance.Status.Provider = p.GetName()
			if err = r.client.Status().Update(ctx, instance); err != nil {
				return reconcile.Result{}, errorUtil.Wrapf(err, "failed to update instance %s in namespace %s", instance.Name, instance.Namespace)
			}
			return reconcile.Result{Requeue: true, RequeueAfter: time.Second * 30}, nil
		}
	}
	return reconcile.Result{}, errorUtil.New(fmt.Sprintf("unsupported deployment strategy %s", stratMap.BlobStorage))
}
