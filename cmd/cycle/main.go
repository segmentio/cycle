package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/segmentio/conf"
	"github.com/segmentio/cycle"
	"github.com/segmentio/cycle/aws"
	_ "github.com/segmentio/errors-go/stderrors"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	_ "github.com/segmentio/events/text"
)

type config struct {
	Cluster      string        `conf:"cluster"       help:"The name or identifier for the cluster to cycle."                                     validate:"nonzero"`
	Environment  string        `conf:"environment"   help:"The cloud environment in which the operation takes place (guessed if not specified)." validate:"nonzero"`
	TargetSize   int           `conf:"target-size"   help:"The target size of the cluster when cycling is complete (guessed if not specified)."  validate:"min=0"`
	MinSize      int           `conf:"min-size"      help:"The minimum size of the cluster being cycled (guessed if not specified)."             validate:"min=0"`
	MaxSize      int           `conf:"max-size"      help:"The maximum size of the cluster being cycled (guessed if not specified)."             validate:"min=0"`
	RunInterval  time.Duration `conf:"run-interval"  help:"Time interval between two cycles."                                                    validate:"min=0"`
	DrainTimeout time.Duration `conf:"drain-timeout" help:"Time limit on how long draining instances may take." validate:"min=1"`
	DryRun       bool          `conf:"dry-run"       help:"Don't cycle the cluster, just output what would be done."`
}

func init() {
	events.DefaultLogger.EnableDebug = true
	events.DefaultLogger.EnableSource = false
}

func main() {
	config := config{
		Environment:  guessEnvironment(),
		DryRun:       false,
		RunInterval:  5 * time.Second,
		DrainTimeout: 2 * time.Minute,
	}

	conf.LoadWith(&config, conf.Loader{
		Name:  "cycle",
		Usage: "-cluster CLUSTER [options...]",
		Args:  os.Args[1:],
	})

	events.Log("cycling %{cluster_name}s", config.Cluster)

	ctx, cancel := events.WithSignals(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, config); err != nil {
		events.Log("%{error}+v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, config config) error {
	var environment cycle.Environment
	switch config.Environment {
	case "aws":
		environment = aws.NewEnvironment(session.New())
	default:
		return fmt.Errorf("unknown environment: %s", config.Environment)
	}
	if config.DryRun {
		environment = cycle.DryRun(environment)
	}
	environment = envLogs{environment}

	clusterID, err := environment.LookupClusterID(ctx, config.Cluster)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(config.RunInterval)
	defer ticker.Stop()

	targetSize := config.TargetSize
	for {
		if cluster, err := environment.DescribeCluster(ctx, clusterID); err != nil {
			events.Log("error describing %{cluster_id}s - %{error}v", clusterID, err)
		} else {
			if targetSize == 0 {
				targetSize = len(cluster.Instances)
			}

			taskConfig := cycle.TaskConfig{
				TargetSize:   targetSize,
				MinSize:      coalesce(config.MinSize, max(cluster.MinSize, fraction(targetSize, 0.8))),
				MaxSize:      coalesce(config.MaxSize, min(cluster.MaxSize, fraction(targetSize, 1.2))),
				DrainTimeout: config.DrainTimeout,
			}

			events.Log("%{cluster_id}s - generating tasks (target size: %{target_size}d, min size: %{min_size}d, max size: %{max_size}d, drain timeout: %{drain_timeout}s)",
				cluster.ID, taskConfig.TargetSize, taskConfig.MinSize, taskConfig.MaxSize, taskConfig.DrainTimeout)

			tasks, err := cycle.Tasks(cluster, taskConfig)
			if err != nil {
				return err
			}

			if len(tasks) == 0 {
				events.Log("done")
				return nil
			}

			if err = cycle.Run(ctx, environment, tasks...); err != nil {
				events.Log("%{error}+v", err)
			}
		}

		if config.DryRun {
			return nil
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func guessEnvironment() string {
	switch {
	case isAWS():
		return "aws"
	default:
		return ""
	}
}

func isAWS() bool {
	return len(os.Getenv("AWS_ACCESS_KEY_ID")) != 0 && len(os.Getenv("AWS_SECRET_ACCESS_KEY")) != 0
}

func coalesce(values ...int) int {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

func fraction(value int, coeff float64) int {
	return int(math.Ceil(float64(value) * coeff))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
