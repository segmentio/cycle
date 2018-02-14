package cycle

import (
	"context"
	"sync"

	errors "github.com/segmentio/errors-go"
)

// Run executes the given list of tasks against env.
func Run(ctx context.Context, env Environment, tasks ...Task) error {
	wg := sync.WaitGroup{}
	wg.Add(len(tasks))

	errch := make(chan error, len(tasks))
	go func() {
		wg.Wait()
		close(errch)
	}()

	for _, t := range tasks {
		go func(t Task) {
			defer wg.Done()
			errch <- t.Run(ctx, env)
		}(t)
	}

	return errors.Recv(errch)
}
