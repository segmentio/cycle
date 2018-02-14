package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/cycle"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	_ "github.com/segmentio/events/text"
)

func init() {
	events.DefaultLogger.EnableDebug = true
	events.DefaultLogger.EnableSource = false
}

func TestRun(t *testing.T) {
	now := time.Now()

	cluster := func(name string, config cycle.ConfigID, instances ...cycle.Instance) cycle.Cluster {
		c := cycle.Cluster{
			ID:        generateClusterID(),
			Config:    config,
			Name:      name,
			MaxSize:   1000,
			Instances: make([]cycle.Instance, len(instances)),
		}
		copy(c.Instances, instances)
		for i := range c.Instances {
			c.Instances[i].ID = generateInstanceID()
		}
		return c
	}

	instance := func(state cycle.InstanceState, config cycle.ConfigID) cycle.Instance {
		return cycle.Instance{
			ID:        generateInstanceID(),
			State:     state,
			Config:    config,
			CreatedAt: now,
			UpdatedAt: now,
		}
	}

	env := testLogger{
		T: t,
		base: NewTestEnvironment(
			cluster("empty-1", "config-1"),
			cluster("empty-2", "config-1"),
			cluster("empty-3", "config-1",
				instance(cycle.Started, "config-1"),
				instance(cycle.Started, "config-1"),
				instance(cycle.Started, "config-1"),
				instance(cycle.Started, "config-1"),
				instance(cycle.Started, "config-1"),
			),
			cluster("scale-in-1", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
			),
			cluster("scale-in-2", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
			),
			cluster("scale-out-1", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
			),
			cluster("scale-out-2", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
			),
			cluster("scale-in-out-1", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
				instance(cycle.Started, "config-0"),
			),
			cluster("scale-in-catch-up-1", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Starting, "config-0"),
				instance(cycle.Draining, "config-0"),
				instance(cycle.Drained, "config-0"),
				instance(cycle.Terminating, "config-0"),
			),
			cluster("scale-in-catch-up-2", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Starting, "config-0"),
				instance(cycle.Draining, "config-0"),
				instance(cycle.Drained, "config-0"),
				instance(cycle.Terminating, "config-0"),
			),
			cluster("scale-out-catch-up-1", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Starting, "config-0"),
				instance(cycle.Draining, "config-0"),
				instance(cycle.Drained, "config-0"),
				instance(cycle.Terminating, "config-0"),
			),
			cluster("scale-out-catch-up-2", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Starting, "config-0"),
				instance(cycle.Draining, "config-0"),
				instance(cycle.Drained, "config-0"),
				instance(cycle.Terminating, "config-0"),
			),
			cluster("scale-in-out-catch-up-1", "config-1",
				instance(cycle.Started, "config-0"),
				instance(cycle.Starting, "config-0"),
				instance(cycle.Draining, "config-0"),
				instance(cycle.Drained, "config-0"),
				instance(cycle.Terminating, "config-0"),
			),
		),
	}

	tests := []struct {
		scenario string
		cluster  string
		config   cycle.TaskConfig
	}{
		{
			scenario: "cycling an empty cluster leaves the cluster empty",
			cluster:  "empty-1",
			config: cycle.TaskConfig{
				TargetSize: 0,
				MinSize:    0,
				MaxSize:    100,
			},
		},

		{
			scenario: "cycling an empty cluster adjusts the number of instances to the target size",
			cluster:  "empty-2",
			config: cycle.TaskConfig{
				TargetSize: 10,
				MinSize:    0,
				MaxSize:    100,
			},
		},

		{
			scenario: "cycling a cluster with a target size of zero terminates all instances",
			cluster:  "empty-3",
			config: cycle.TaskConfig{
				TargetSize: 0,
				MinSize:    0,
				MaxSize:    100,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can terminate all its instances",
			cluster:  "scale-in-1",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    0,
				MaxSize:    5,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can terminate one instance",
			cluster:  "scale-in-2",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    4,
				MaxSize:    5,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can double its instance count",
			cluster:  "scale-out-1",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    5,
				MaxSize:    10,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can start only one instance",
			cluster:  "scale-out-2",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    5,
				MaxSize:    6,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can start and terminate half its instance count",
			cluster:  "scale-in-out-1",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    3,
				MaxSize:    8,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can terminate all its instances while catching up on mixed instance states",
			cluster:  "scale-in-catch-up-1",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    0,
				MaxSize:    5,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can terminate one instance while catching up on mixed instance states",
			cluster:  "scale-in-catch-up-2",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    4,
				MaxSize:    5,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can double its instance count while catching up on mixed instance states",
			cluster:  "scale-out-catch-up-1",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    5,
				MaxSize:    10,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can start only one instance while catching up on mixed instance states",
			cluster:  "scale-out-catch-up-2",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    5,
				MaxSize:    6,
			},
		},

		{
			scenario: "cycling succeeds if the cluster can start and terminate half its instance count while catching up on mixed instance states",
			cluster:  "scale-in-out-catch-up-1",
			config: cycle.TaskConfig{
				TargetSize: 5,
				MinSize:    3,
				MaxSize:    8,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			testRun(t, env, test.cluster, test.config)
		})
	}
}

func testRun(t *testing.T, env cycle.Environment, name string, config cycle.TaskConfig) {
	id, err := env.LookupClusterID(context.Background(), name)
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i != 100; i++ {
		cluster, err := env.DescribeCluster(context.Background(), id)
		if err != nil {
			t.Error(err)
			continue
		}

		tasks, err := cycle.Tasks(cluster, config)
		if err != nil {
			t.Error(err)
			continue
		}

		if len(tasks) == 0 {
			goto validate
		}

		if err := cycle.Run(context.Background(), env, tasks...); err != nil {
			t.Error(err)
			continue
		}
	}

	t.Error("the drain did not complete within 100 iterations, something must be wrong")
validate:
	cluster, err := env.DescribeCluster(context.Background(), id)
	if err != nil {
		t.Error(err)
		return
	}

	if len(cluster.Instances) != config.TargetSize {
		t.Error("instance count mismatch")
		t.Log("expected:", config.TargetSize)
		t.Log("found:   ", len(cluster.Instances))
	}

	for _, instance := range cluster.Instances {
		if instance.State != cycle.Started {
			t.Errorf("instance %s is in an invalid state, expected %q but found %q", instance.ID, cycle.Started, instance.State)
		}
	}
}
