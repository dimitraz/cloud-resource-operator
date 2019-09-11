package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/elasticache/elasticacheiface"
	"github.com/integr8ly/cloud-resource-operator/pkg/apis/integreatly/v1alpha1"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	v1 "github.com/openshift/cloud-credential-operator/pkg/apis/cloudcredential/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"testing"
)

type mockElasticacheClient struct {
	elasticacheiface.ElastiCacheAPI
}

func (m *mockElasticacheClient) DescribeReplicationGroups(*elasticache.DescribeReplicationGroupsInput) (*elasticache.DescribeReplicationGroupsOutput, error) {
	return &elasticache.DescribeReplicationGroupsOutput{}, nil
}

func TestAWSRedisProvider_CreateRedis(t *testing.T) {
	scheme := runtime.NewScheme()
	err := v1.AddToScheme(scheme)
	err = corev1.AddToScheme(scheme)
	err = v1alpha1.SchemeBuilder.AddToScheme(scheme)
	if err != nil {
		t.Fatal("failed to build scheme", err)
	}

	sc := &StrategyConfig{
		Region:      "eu-west-1",
		RawStrategy: json.RawMessage("{}"),
	}
	rawStratCfg, err := json.Marshal(sc)
	if err != nil {
		t.Fatal("failed to marshal strategy config", err)
	}

	cases := []struct {
		name          string
		instance      *v1alpha1.Redis
		client        client.Client
		configMgr     *ConfigManagerInterfaceMock
		credentialMgr *CredentialManagerInterfaceMock
	}{
		{
			name: "test defaults replication group id is set",
			client: fake.NewFakeClientWithScheme(scheme, &v1alpha1.Redis{
				ObjectMeta: controllerruntime.ObjectMeta{
					Name:      "test",
					Namespace: "test",
				},
			}, &corev1.ConfigMap{
				ObjectMeta: controllerruntime.ObjectMeta{
					Name:      "cloud-resources-aws-strategies",
					Namespace: "kube-system",
				},
				Data: map[string]string{
					"redis": fmt.Sprintf("{\"test\": %s}", string(rawStratCfg)),
				},
			}),
			instance: &v1alpha1.Redis{
				ObjectMeta: controllerruntime.ObjectMeta{
					Name:      "test",
					Namespace: "test",
				},
			},
			configMgr: &ConfigManagerInterfaceMock{
				ReadStorageStrategyFunc: func(ctx context.Context, rt providers.ResourceType, tier string) (config *StrategyConfig, e error) {
					return sc, nil
				},
			},
			credentialMgr: &CredentialManagerInterfaceMock{
				ReconcileBucketOwnerCredentialsFunc: nil,
				ReconcileCredentialsFunc: func(ctx context.Context, name string, ns string, entries []v1.StatementEntry) (request *v1.CredentialsRequest, credentials *AWSCredentials, e error) {
					return &v1.CredentialsRequest{}, &AWSCredentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
				},
				ReconcileProviderCredentialsFunc: func(ctx context.Context, ns string) (credentials *AWSCredentials, e error) {
					return &AWSCredentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &AWSRedisProvider{
				Client:            tc.client,
				CredentialManager: tc.credentialMgr,
				ConfigManager:     tc.configMgr,
			}
			redis, err := p.CreateRedis(context.TODO(), tc.instance)
			if err != nil {
				t.Fatal("", err)
			}

			// just testing if it works
			t.Fatal("", redis)
		})
	}
}
