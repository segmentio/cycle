package cycle

import (
	"context"
	"sort"
	"time"
)

// The Environment interface abstracts the environment that controls the actual
// instance clusters that are to be cycled.
//
// Implementations of the Environment interface must be safe to use concurrently
// from multiple goroutines.
type Environment interface {
	// LookupClusterID takes a cluster name as argument, returns the matching
	// cluster ID.
	LookupClusterID(context.Context, string) (ClusterID, error)

	// DescribeCluster returns the full cluster information for the cluster
	// identified by the given ID.
	DescribeCluster(context.Context, ClusterID) (Cluster, error)

	// StartInstances stats count instances in the cluster identified by the
	// given ID, returns instance values representing the newly created
	// instances.
	StartInstances(context.Context, ClusterID, int) error

	// DrainInstance puts the instance identified by the given ID in draining
	// state.
	DrainInstance(context.Context, InstanceID) error

	// TerminateInstance terminates the instance identified by the given ID.
	TerminateInstance(context.Context, InstanceID) error

	// WaitInstanceState is called when the cluster cycling algorithm is waiting
	// for a given instance to enter a specific state.
	WaitInstanceState(context.Context, InstanceID, InstanceState) error
}

// ConfigID is a type representing a cluster configuration ID.
type ConfigID string

// ClusterID is a type representing a cluster ID.
type ClusterID string

// Cluster carries attributes of a cluster that are needed to run the cycling
// algorithm.
type Cluster struct {
	ID        ClusterID
	Config    ConfigID
	Name      string
	MinSize   int
	MaxSize   int
	Instances []Instance
}

// InstanceID is a type representing an instance ID.
type InstanceID string

// InstsanceState is an enumeration type representing the various states that
// instances may be in.
type InstanceState string

const (
	Starting    InstanceState = "starting"
	Started     InstanceState = "started"
	Draining    InstanceState = "draining"
	Drained     InstanceState = "drained"
	Terminating InstanceState = "terminating"
	Terminated  InstanceState = "terminated" // impossible
)

// Instance carries attributes of an instance that are needed to run the cycling
// algorithm.
type Instance struct {
	ID        InstanceID
	State     InstanceState
	Config    ConfigID
	CreatedAt time.Time
	UpdatedAt time.Time
}

// DryRun is an Environment decorator which only enables read-only operations on
// env.
func DryRun(env Environment) Environment {
	return dryRun{base: env}
}

type dryRun struct {
	base Environment
}

func (env dryRun) LookupClusterID(ctx context.Context, name string) (ClusterID, error) {
	return env.base.LookupClusterID(ctx, name)
}

func (env dryRun) DescribeCluster(ctx context.Context, id ClusterID) (Cluster, error) {
	return env.base.DescribeCluster(ctx, id)
}

func (dryRun) StartInstances(ctx context.Context, cluster ClusterID, count int) error {
	return ctx.Err()
}

func (dryRun) DrainInstance(ctx context.Context, instance InstanceID) error {
	return ctx.Err()
}

func (dryRun) TerminateInstance(ctx context.Context, instance InstanceID) error {
	return ctx.Err()
}

func (dryRun) WaitInstanceState(ctx context.Context, instance InstanceID, state InstanceState) error {
	return ctx.Err()
}

func sortedInstancesByCreatedAt(instances []Instance) []Instance {
	return sortedInstances(instances, func(i1, i2 Instance) bool {
		return i1.CreatedAt.Before(i2.CreatedAt)
	})
}

func sortedInstances(instances []Instance, sortBy func(Instance, Instance) bool) []Instance {
	instances = copyInstances(instances)
	sort.Slice(instances, func(i, j int) bool {
		return sortBy(instances[i], instances[j])
	})
	return instances
}

func copyInstances(instances []Instance) []Instance {
	instancesCopy := make([]Instance, len(instances))
	copy(instancesCopy, instances)
	return instancesCopy
}
