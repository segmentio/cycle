package cycle_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/cycle"
	errors "github.com/segmentio/errors-go"
)

type TestEnvironment struct {
	mutex    sync.Mutex
	clusters []cycle.Cluster
}

func NewTestEnvironment(clusters ...cycle.Cluster) *TestEnvironment {
	copyClusters := make([]cycle.Cluster, len(clusters))
	copy(copyClusters, clusters)
	return &TestEnvironment{clusters: copyClusters}
}

func (env *TestEnvironment) LookupClusterID(ctx context.Context, name string) (cycle.ClusterID, error) {
	env.mutex.Lock()
	defer env.mutex.Unlock()

	c, err := env.clusterByName(ctx, name)
	if err != nil {
		return "", err
	}

	return c.ID, nil
}

func (env *TestEnvironment) DescribeCluster(ctx context.Context, id cycle.ClusterID) (cycle.Cluster, error) {
	env.mutex.Lock()
	defer env.mutex.Unlock()

	c, err := env.clusterByID(ctx, id)
	if err != nil {
		return cycle.Cluster{}, err
	}

	ret := *c
	ret.Instances = filterInstances(ret.Instances, func(_ int) bool { return true })
	return ret, nil
}

func (env *TestEnvironment) StartInstances(ctx context.Context, cluster cycle.ClusterID, count int) error {
	env.mutex.Lock()
	defer env.mutex.Unlock()

	c, err := env.clusterByID(ctx, cluster)
	if err != nil {
		return err
	}

	now := time.Now()

	for i := 0; i < count; i++ {
		c.Instances = append(c.Instances, cycle.Instance{
			ID:        generateInstanceID(),
			State:     cycle.Starting,
			Config:    c.Config,
			CreatedAt: now,
			UpdatedAt: now,
		})
	}

	return nil
}

func (env *TestEnvironment) DrainInstance(ctx context.Context, instance cycle.InstanceID) error {
	env.mutex.Lock()
	defer env.mutex.Unlock()

	c, i, err := env.instanceByID(ctx, instance)
	if err != nil {
		if errors.Is("NotFound", err) {
			return nil
		}
		return err
	}

	c.Instances[i].State = cycle.Draining
	c.Instances[i].UpdatedAt = time.Now()
	return nil
}

func (env *TestEnvironment) TerminateInstance(ctx context.Context, instance cycle.InstanceID) error {
	env.mutex.Lock()
	defer env.mutex.Unlock()

	c, i, err := env.instanceByID(ctx, instance)
	if err != nil {
		if errors.Is("NotFound", err) {
			return nil
		}
		return err
	}

	c.Instances[i].State = cycle.Terminating
	c.Instances[i].UpdatedAt = time.Now()
	return nil
}

func (env *TestEnvironment) WaitInstanceState(ctx context.Context, instance cycle.InstanceID, state cycle.InstanceState) error {
	env.mutex.Lock()
	defer env.mutex.Unlock()

	c, i, err := env.instanceByID(ctx, instance)
	if err != nil {
		if errors.Is("NotFound", err) {
			return nil
		}
		return err
	}

	switch state {
	case cycle.Started:
		if c.Instances[i].State == cycle.Starting {
			c.Instances[i].State = cycle.Started
			c.Instances[i].UpdatedAt = time.Now()
		}

	case cycle.Drained:
		if c.Instances[i].State == cycle.Draining {
			c.Instances[i].State = cycle.Drained
			c.Instances[i].UpdatedAt = time.Now()
		}

	case cycle.Terminated:
		if c.Instances[i].State == cycle.Terminating {
			c.Instances = filterInstances(c.Instances, func(j int) bool { return i != j })
		}
	}

	return nil
}

func (env *TestEnvironment) clusterByName(ctx context.Context, cluster string) (*cycle.Cluster, error) {
	return env.clusterBy(ctx, func(c cycle.Cluster) bool { return c.Name == cluster })
}

func (env *TestEnvironment) clusterByID(ctx context.Context, cluster cycle.ClusterID) (*cycle.Cluster, error) {
	return env.clusterBy(ctx, func(c cycle.Cluster) bool { return c.ID == cluster })
}

func (env *TestEnvironment) clusterBy(ctx context.Context, predicate func(cycle.Cluster) bool) (*cycle.Cluster, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i, c := range env.clusters {
		if predicate(c) {
			return &env.clusters[i], nil
		}
	}
	return nil, errors.WithTypes(errors.New("no such cluster"), "NotFound")
}

func (env *TestEnvironment) instanceByID(ctx context.Context, instance cycle.InstanceID) (*cycle.Cluster, int, error) {
	return env.instanceBy(ctx, func(i cycle.Instance) bool { return i.ID == instance })
}

func (env *TestEnvironment) instanceBy(ctx context.Context, predicate func(cycle.Instance) bool) (*cycle.Cluster, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	for i, c := range env.clusters {
		for j := range c.Instances {
			if predicate(c.Instances[j]) {
				return &env.clusters[i], j, nil
			}
		}
	}
	return nil, 0, errors.WithTypes(errors.New("no such instance"), "NotFound")
}

func filterInstances(instances []cycle.Instance, predicate func(int) bool) []cycle.Instance {
	instancesCopy := make([]cycle.Instance, 0, len(instances))
	for i := range instances {
		if predicate(i) {
			instancesCopy = append(instancesCopy, instances[i])
		}
	}
	return instancesCopy
}

func generateClusterID() cycle.ClusterID {
	return cycle.ClusterID(fmt.Sprintf("c-%08x", rand.Int63n(0x7FFFFFFF)))
}

func generateInstanceID() cycle.InstanceID {
	return cycle.InstanceID(fmt.Sprintf("i-%08x", rand.Int63n(0x7FFFFFFF)))
}

type testLogger struct {
	*testing.T
	base cycle.Environment
}

func (env testLogger) LookupClusterID(ctx context.Context, name string) (cycle.ClusterID, error) {
	clusterID, err := env.base.LookupClusterID(ctx, name)
	if err != nil {
		env.Logf("error looking up %s cluster - %v", name, err)
	} else {
		env.Logf("%s - found %s cluster", clusterID, name)
	}
	return clusterID, err
}

func (env testLogger) DescribeCluster(ctx context.Context, id cycle.ClusterID) (cycle.Cluster, error) {
	cluster, err := env.base.DescribeCluster(ctx, id)
	if err != nil {
		env.Logf("%s - error describing cluster - %v", id, err)
	} else {
		outdated := 0

		for _, instance := range cluster.Instances {
			if instance.Config != cluster.Config {
				outdated++
			}
		}

		env.Logf("%s - found configuration %s and %d/%d outdated instances",
			id, cluster.Config, outdated, len(cluster.Instances))
	}
	return cluster, err
}

func (env testLogger) StartInstances(ctx context.Context, cluster cycle.ClusterID, count int) error {
	env.Logf("%s - starting %d new instances", cluster, count)
	err := env.base.StartInstances(ctx, cluster, count)
	if err != nil {
		env.Logf("%s - error starting %d instances - %v", cluster, count, err)
	}
	return err
}

func (env testLogger) DrainInstance(ctx context.Context, instance cycle.InstanceID) error {
	env.Logf("%s - draining", instance)
	err := env.base.DrainInstance(ctx, instance)
	if err != nil {
		env.Logf("%s - error draining - %v", instance, err)
	}
	return err
}

func (env testLogger) TerminateInstance(ctx context.Context, instance cycle.InstanceID) error {
	env.Logf("%s - terminating", instance)
	err := env.base.TerminateInstance(ctx, instance)
	if err != nil {
		env.Logf("%s - error terminating - %v", instance, err)
	}
	return nil
}

func (env testLogger) WaitInstanceState(ctx context.Context, instance cycle.InstanceID, state cycle.InstanceState) error {
	env.Logf("%s - waiting to be %s", instance, state)
	err := env.base.WaitInstanceState(ctx, instance, state)
	if err != nil {
		env.Logf("%s - error waiting to be %s - %v", instance, state, err)
	}
	return err
}
