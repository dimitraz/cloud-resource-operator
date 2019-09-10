package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/integr8ly/cloud-resource-operator/pkg/apis/integreatly/v1alpha1"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/integr8ly/cloud-resource-operator/pkg/providers"

	errorUtil "github.com/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AWSRedisDeploymentDetails provider specific details about the AWS Redis Cluster created
type AWSRedisDeploymentDetails struct {
	Connection *elasticache.Endpoint
}

func (d *AWSRedisDeploymentDetails) Data() *elasticache.Endpoint {
	return d.Connection
}

// AWS Redis Provider implementation for AWS Elasticache
type AWSRedisProvider struct {
	Client            client.Client
	CredentialManager *CredentialManager
	ConfigManager     *ConfigManager
}

func NewAWSRedisProvider(client client.Client) *AWSRedisProvider {
	return &AWSRedisProvider{
		Client:            client,
		CredentialManager: NewCredentialManager(client),
		ConfigManager:     NewDefaultConfigManager(client),
	}
}

func (p *AWSRedisProvider) GetName() string {
	return providers.AWSDeploymentStrategy
}

func (p *AWSRedisProvider) SupportsStrategy(d string) bool {
	return d == providers.AWSDeploymentStrategy
}

func (p *AWSRedisProvider) CreateRedis(ctx context.Context, r *v1alpha1.Redis) (*providers.RedisCluster, error) {
	// handle provider-specific finalizer
	if r.GetDeletionTimestamp() == nil {
		resources.AddFinalizer(&r.ObjectMeta, defaultFinalizer)
		if err := p.Client.Update(ctx, r); err != nil {
			return nil, errorUtil.Wrapf(err, "failed to add finalizer to instance")
		}
	}

	// info about the redis cluster to be created
	redisConfig, stratCfg, err := p.getRedisConfig(ctx, r)
	if err != nil {
		return nil, errorUtil.Wrapf(err, "failed to retrieve aws redis cluster config for instance %s", r.Name)
	}
	if redisConfig.ReplicationGroupId == nil {
		redisConfig.ReplicationGroupId = aws.String(fmt.Sprintf("%s-%s", r.Namespace, r.Name))
	}

	// create the credentials to be used by the aws resource providers, not to be used by end-user
	providerCreds, err := p.CredentialManager.ReconcileProviderCredentials(ctx, r.Namespace)
	if err != nil {
		return nil, errorUtil.Wrap(err, "failed to reconcile s3 put object credentials")
	}

	// setup aws redis cluster sdk session
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(stratCfg.Region),
		Credentials: credentials.NewStaticCredentials(providerCreds.AccessKeyID, providerCreds.SecretAccessKey, ""),
	}))
	cacheSvc := elasticache.New(sess)
	descInput := &elasticache.DescribeReplicationGroupsInput{}

	// the aws access key can sometimes still not be registered in aws on first try, so loop
	var rgs []*elasticache.ReplicationGroup
	err = wait.PollImmediate(time.Second*5, time.Minute*5, func() (done bool, err error) {
		listOutput, err := cacheSvc.DescribeReplicationGroups(descInput)
		if err != nil {
			return false, nil
		}
		rgs = listOutput.ReplicationGroups
		return true, nil
	})
	if err != nil {
		return nil, errorUtil.Wrapf(err, "timed out waiting to get replication groups")
	}

	// pre-create the redis cluster that will be returned if everything is successful
	redis := &providers.RedisCluster{
		DeploymentDetails: &AWSRedisDeploymentDetails{
			Connection: &elasticache.Endpoint{},
		},
	}

	// check if the cluster has already been created
	var foundCache *elasticache.ReplicationGroup
	for _, c := range rgs {
		if *c.ReplicationGroupId == *redisConfig.ReplicationGroupId {
			foundCache = c
			break
		}
	}
	if foundCache != nil {
		redis.DeploymentDetails = &AWSRedisDeploymentDetails{
			Connection: foundCache.ConfigurationEndpoint,
		}
		return redis, nil
	}

	// if it hasn't been created, create the redis cluster
	input := &elasticache.CreateReplicationGroupInput{
		AutomaticFailoverEnabled:    aws.Bool(true),
		CacheNodeType:               aws.String("cache.t2.micro"),
		Engine:                      aws.String("redis"),
		EngineVersion:               aws.String("2.8.24"),
		NumCacheClusters:            aws.Int64(3),
		ReplicationGroupDescription: aws.String("A Redis replication group."),
		ReplicationGroupId:          aws.String(*redisConfig.ReplicationGroupId),
		SnapshotRetentionLimit:      aws.Int64(30),
	}
	rg, err := cacheSvc.CreateReplicationGroup(input)
	if err != nil {
		return nil, err
	}

	// update the redis connection endpoint
	redis.DeploymentDetails = &AWSRedisDeploymentDetails{
		Connection: rg.ReplicationGroup.ConfigurationEndpoint,
	}
	return redis, nil
}

func (p *AWSRedisProvider) DeleteRedis(ctx context.Context) error {
	return nil
}

func (p *AWSRedisProvider) getRedisConfig(ctx context.Context, r *v1alpha1.Redis) (*elasticache.CreateReplicationGroupInput, *StrategyConfig, error) {
	stratCfg, err := p.ConfigManager.ReadStorageStrategy(ctx, providers.RedisResourceType, r.Spec.Tier)
	if err != nil {
		return nil, nil, errorUtil.Wrap(err, "failed to read aws strategy config")
	}
	if stratCfg.Region == "" {
		stratCfg.Region = defaultRegion
	}

	// unmarshal the redis cluster config
	redisConfig := &elasticache.CreateReplicationGroupInput{}
	if err := json.Unmarshal(stratCfg.RawStrategy, redisConfig); err != nil {
		return nil, nil, errorUtil.Wrap(err, "failed to unmarshal aws redis cluster configuration")
	}
	return redisConfig, stratCfg, nil
}
