package worker

import (
	"context"
	"time"
)

// Task represents a unit of work that can be executed by a worker pool.
// Implementations must be thread-safe as they may be executed concurrently.
type Task interface {
	// Execute performs the task's main work. It should respect context cancellation.
	// Returns an error if the task fails.
	Execute(ctx context.Context) error

	// OnSuccess is called after successful task execution.
	// This method is optional and can be a no-op.
	OnSuccess(ctx context.Context)

	// OnFailure is called after task execution fails.
	// The error parameter contains the failure reason.
	// This method is optional and can be a no-op.
	OnFailure(ctx context.Context, err error)

	// Name returns a human-readable name for the task.
	// Used for logging and debugging purposes.
	Name() string
}

// BaseTask provides a default implementation of Task interface.
// Embed this in your custom tasks to inherit default behavior.
type BaseTask struct {
	name string
}

// NewBaseTask creates a new BaseTask with the given name.
func NewBaseTask(name string) *BaseTask {
	return &BaseTask{name: name}
}

// Execute is a no-op implementation. Override this in your custom task.
func (t *BaseTask) Execute(ctx context.Context) error {
	return nil
}

// OnSuccess is a no-op implementation. Override this if needed.
func (t *BaseTask) OnSuccess(ctx context.Context) {}

// OnFailure is a no-op implementation. Override this if needed.
func (t *BaseTask) OnFailure(ctx context.Context, err error) {}

// Name returns the task name.
func (t *BaseTask) Name() string {
	return t.name
}

// FuncTask is a Task implementation that wraps a simple function.
// Useful for quick task creation without defining a new type.
type FuncTask struct {
	name       string
	fn         func(ctx context.Context) error
	onSuccess  func(ctx context.Context)
	onFailure  func(ctx context.Context, err error)
	createdAt  time.Time
}

// NewFuncTask creates a task from a function.
func NewFuncTask(name string, fn func(ctx context.Context) error) *FuncTask {
	return &FuncTask{
		name:       name,
		fn:         fn,
		onSuccess:  func(ctx context.Context) {},
		onFailure:  func(ctx context.Context, err error) {},
		createdAt:  time.Now(),
	}
}

// WithOnSuccess sets the success callback.
func (t *FuncTask) WithOnSuccess(fn func(ctx context.Context)) *FuncTask {
	t.onSuccess = fn
	return t
}

// WithOnFailure sets the failure callback.
func (t *FuncTask) WithOnFailure(fn func(ctx context.Context, err error)) *FuncTask {
	t.onFailure = fn
	return t
}

// Execute runs the wrapped function.
func (t *FuncTask) Execute(ctx context.Context) error {
	return t.fn(ctx)
}

// OnSuccess calls the success callback.
func (t *FuncTask) OnSuccess(ctx context.Context) {
	t.onSuccess(ctx)
}

// OnFailure calls the failure callback.
func (t *FuncTask) OnFailure(ctx context.Context, err error) {
	t.onFailure(ctx, err)
}

// Name returns the task name.
func (t *FuncTask) Name() string {
	return t.name
}

// CreatedAt returns when the task was created.
func (t *FuncTask) CreatedAt() time.Time {
	return t.createdAt
}
