package cycle

import (
	"context"
	"fmt"
	"time"
)

// The Task interface abstracts the execution of a single operation on an
// environment.
//
// Programs typically generate tasks by calling the Tasks function and passing
// configuration and constraints to determine what operations can be executed.
type Task interface {
	Run(ctx context.Context, env Environment) error
}

// TaskConfig is a configuration object used to generate a list of tasks to be
// executed when calling the Tasks function.
type TaskConfig struct {
	// The final desired size of the cluster.
	TargetSize int

	// The minimum instance count in the cluster.
	MinSize int

	// The maximum instance count in the cluster.
	MaxSize int

	// The time limit on how long instances may take to drain.
	DrainTimeout time.Duration
}

// Tasks generates a list of tasks that represents the next steps that can be
// taken to cycle the given cluster, using the config value to apply constraints
// on the operations.
//
// The function returns an empty list of tasks if there is no more work left to
// be done to cycle the cluster.
func Tasks(cluster Cluster, config TaskConfig) ([]Task, error) {
	if config.TargetSize < 0 {
		return nil, fmt.Errorf("the target size cannot be a negative value (%d)", config.TargetSize)
	}
	if config.MinSize < 0 {
		return nil, fmt.Errorf("the minimum size cannot be a negative value (%d)", config.MinSize)
	}
	if config.MaxSize < 0 {
		return nil, fmt.Errorf("the maximum size cannot be a negative value (%d)", config.MaxSize)
	}
	if config.DrainTimeout < 0 {
		return nil, fmt.Errorf("the drain timeout cannot be a negative value (%s)", config.DrainTimeout)
	}
	if config.MinSize >= config.MaxSize {
		return nil, fmt.Errorf("the min cluster size (%d) must be less than the max cluster size (%d)", config.MinSize, config.MaxSize)
	}

	var now = time.Now()
	var tasks []Task
	var started int
	var outdated int

	for _, instance := range cluster.Instances {
		switch instance.State {
		case Starting:
			tasks = append(tasks, wait(instance.ID, Started))

		case Started:
			started++

		case Draining:
			if !instance.UpdatedAt.IsZero() && config.DrainTimeout != 0 && now.Sub(instance.UpdatedAt) >= config.DrainTimeout {
				tasks = append(tasks, terminate(instance.ID))
			} else {
				tasks = append(tasks, wait(instance.ID, Drained))
			}

		case Drained:
			tasks = append(tasks, terminate(instance.ID))

		case Terminating:
			tasks = append(tasks, wait(instance.ID, Terminated))
		}
	}

	for _, instance := range cluster.Instances {
		if instance.Config != cluster.Config {
			outdated++
			if instance.State == Started && started > config.MinSize {
				tasks = append(tasks, drain(instance.ID))
				started--
			}
		}
	}

	if outdated != 0 {
		if len(cluster.Instances) < config.MaxSize {
			count := config.MaxSize - len(cluster.Instances)
			tasks = append(tasks, start(cluster.ID, count))
		}
	} else if len(tasks) == 0 {
		switch {
		case len(cluster.Instances) < config.TargetSize:
			count := config.TargetSize - len(cluster.Instances)
			tasks = append(tasks, start(cluster.ID, count))

		case len(cluster.Instances) > config.TargetSize:
			count := len(cluster.Instances) - config.TargetSize

			for _, instance := range sortedInstancesByCreatedAt(cluster.Instances) {
				if count == 0 {
					break
				}
				tasks = append(tasks, drain(instance.ID))
				count--
			}
		}
	}

	return tasks, nil
}

func start(cluster ClusterID, count int) Task {
	return taskFunc(func(ctx context.Context, env Environment) error { return env.StartInstances(ctx, cluster, count) })
}

func drain(instance InstanceID) Task {
	return taskFunc(func(ctx context.Context, env Environment) error { return env.DrainInstance(ctx, instance) })
}

func terminate(instance InstanceID) Task {
	return taskFunc(func(ctx context.Context, env Environment) error { return env.TerminateInstance(ctx, instance) })
}

func wait(instance InstanceID, state InstanceState) Task {
	return taskFunc(func(ctx context.Context, env Environment) error { return env.WaitInstanceState(ctx, instance, state) })
}

type taskFunc func(ctx context.Context, env Environment) error

func (f taskFunc) Run(ctx context.Context, env Environment) error { return f(ctx, env) }
