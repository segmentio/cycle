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
	var toDrain []InstanceID
	var toTerm []InstanceID
	var toWait = map[InstanceState][]InstanceID{}

	for _, instance := range cluster.Instances {
		switch instance.State {
		case Starting:
			toWait[Started] = append(toWait[Started], instance.ID)

		case Started:
			started++

		case Draining:
			if !instance.UpdatedAt.IsZero() && config.DrainTimeout != 0 && now.Sub(instance.UpdatedAt) >= config.DrainTimeout {
				toTerm = append(toTerm, instance.ID)
			} else {
				toWait[Drained] = append(toWait[Drained], instance.ID)
			}

		case Drained:
			toTerm = append(toTerm, instance.ID)

		case Terminating:
			toWait[Terminated] = append(toWait[Terminated], instance.ID)
		}
	}

	for _, instance := range cluster.Instances {
		if instance.Config != cluster.Config {
			outdated++
			if instance.State == Started && started > config.MinSize {
				toDrain = append(toDrain, instance.ID)
				started--
			}
		}
	}

	if outdated != 0 {
		if len(cluster.Instances) < config.MaxSize {
			count := config.MaxSize - len(cluster.Instances)
			tasks = append(tasks, start(cluster.ID, count))
		}
	} else if len(toDrain) == 0 && len(toTerm) == 0 && len(toWait) == 0 {
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
				toDrain = append(toDrain, instance.ID)
				count--
			}
		}
	}

	if len(toDrain) != 0 {
		tasks = append(tasks, drain(toDrain))
	}

	if len(toTerm) != 0 {
		tasks = append(tasks, terminate(toTerm))
	}

	for state, instances := range toWait {
		tasks = append(tasks, wait(state, instances))
	}

	return tasks, nil
}

func start(cluster ClusterID, count int) Task {
	return taskFunc(func(ctx context.Context, env Environment) error { return env.StartInstances(ctx, cluster, count) })
}

func drain(instances []InstanceID) Task {
	return taskFunc(func(ctx context.Context, env Environment) error { return env.DrainInstances(ctx, instances...) })
}

func terminate(instances []InstanceID) Task {
	return taskFunc(func(ctx context.Context, env Environment) error { return env.TerminateInstances(ctx, instances...) })
}

func wait(state InstanceState, instances []InstanceID) Task {
	return taskFunc(func(ctx context.Context, env Environment) error { return env.WaitInstances(ctx, state, instances...) })
}

type taskFunc func(ctx context.Context, env Environment) error

func (f taskFunc) Run(ctx context.Context, env Environment) error { return f(ctx, env) }
