package addservice

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	"ray.vhatt/todo-gokit/pkg/models"
)

// Middleware describe a service (as opposed to endpoint) middleware.
type Middleware func(Service) Service

// LoggingMiddleware takes a logger as a dependency
// and returns a service Middleware
func LoggingMiddleware(logger log.Logger) Middleware {
	return func(next Service) Service {
		return loggingMiddleware{logger, next}
	}
}

type loggingMiddleware struct {
	logger log.Logger
	next   Service
}

func (mw loggingMiddleware) Sum(ctx context.Context, a, b int) (v int, err error) {
	defer func() {
		mw.logger.Log("method", "Sum", "a", a, "b", b, "v", v, "err", err)
	}()

	return mw.next.Sum(ctx, a, b)
}

func (mw loggingMiddleware) Concat(ctx context.Context, a, b string) (v string, err error) {
	defer func() {
		mw.logger.Log("method", "Concat", "a", a, "b", b, "v", v, "err", err)
	}()
	return mw.next.Concat(ctx, a, b)
}

func (mw loggingMiddleware) Ping(ctx context.Context) (v string, err error) {
	defer func() {
		mw.logger.Log("method", "Ping", "v", v, "err", err)
	}()

	return mw.next.Ping(ctx)
}

func (mw loggingMiddleware) AddToDo(ctx context.Context, task models.ToDoItem) (v string, err error) {
	defer func() {
		mw.logger.Log("method", "AddToDo", "task", task, "v", v, "err", err)
	}()
	v, err = mw.next.AddToDo(ctx, task)
	return
}

func (mw loggingMiddleware) CompleteToDo(ctx context.Context, taskID string) (v string, err error) {
	defer func() {
		mw.logger.Log("method", "CompleteTod", "taskID", taskID, "v", v, "err", err)
	}()
	v, err = mw.next.CompleteToDo(ctx, taskID)
	return
}

func (mw loggingMiddleware) UnDoToDo(ctx context.Context, taskID string) (v string, err error) {
	defer func() {
		mw.logger.Log("method", "UnDoTodo", "taskID", taskID, "v", v, "err", err)
	}()
	v, err = mw.next.UnDoToDo(ctx, taskID)
	return
}

func (mw loggingMiddleware) DeleteToDo(ctx context.Context, taskID string) (v string, err error) {
	defer func() {
		mw.logger.Log("method", "DeleteToDo", "taskID", taskID, "v", v, "err", err)
	}()
	v, err = mw.next.DeleteToDo(ctx, taskID)
	return
}

func (mw loggingMiddleware) GetAllToDo(ctx context.Context) (results []models.ToDoItem, err error) {
	defer func() {
		mw.logger.Log("method", "GetAllToDo", "results", results, "err", err)
	}()
	results, err = mw.next.GetAllToDo(ctx)
	return
}

// InstrumentingMiddleware returns a service middleware that instruments
// the number of integers summed and characters concatenated over the lifetime of
// the service.
func InstrumentingMiddleware(ints, chars metrics.Counter, cubToDo, getTodo metrics.Histogram) Middleware {
	return func(next Service) Service {
		return instrumentingMiddleware{
			ints:    ints,
			chars:   chars,
			cubToDo: cubToDo,
			getToDo: getTodo,
			next:    next,
		}
	}
}

type instrumentingMiddleware struct {
	ints  metrics.Counter
	chars metrics.Counter
	// CRUB without R.
	cubToDo metrics.Histogram
	getToDo metrics.Histogram
	next    Service
}

func (mw instrumentingMiddleware) Sum(ctx context.Context, a, b int) (int, error) {
	v, err := mw.next.Sum(ctx, a, b)
	mw.ints.Add(float64(v))
	return v, err
}

func (mw instrumentingMiddleware) Concat(ctx context.Context, a, b string) (string, error) {
	v, err := mw.next.Concat(ctx, a, b)
	mw.chars.Add(float64(len(v)))
	return v, err
}

func (mw instrumentingMiddleware) Ping(ctx context.Context) (string, error) {
	v, err := mw.next.Ping(ctx)
	mw.chars.Add(1)
	return v, err
}

func (mw instrumentingMiddleware) AddToDo(ctx context.Context, task models.ToDoItem) (v string, err error) {
	defer func(begin time.Time) {
		lvs := []string{"method", "AddToDo", "error", fmt.Sprint(err != nil)}
		mw.cubToDo.With(lvs...).Observe(time.Since(begin).Seconds())
	}(time.Now())
	v, err = mw.next.AddToDo(ctx, task)
	return
}

func (mw instrumentingMiddleware) CompleteToDo(ctx context.Context, taskID string) (v string, err error) {
	defer func(begin time.Time) {
		lvs := []string{"method", "CompleteToDo", "error", fmt.Sprint(err != nil)}
		mw.cubToDo.With(lvs...).Observe(time.Since(begin).Seconds())
	}(time.Now())
	v, err = mw.next.CompleteToDo(ctx, taskID)
	return
}

func (mw instrumentingMiddleware) UnDoToDo(ctx context.Context, taskID string) (v string, err error) {
	defer func(begin time.Time) {
		lvs := []string{"method", "UnDoToDo", "error", fmt.Sprint(err != nil)}
		mw.cubToDo.With(lvs...).Observe(time.Since(begin).Seconds())
	}(time.Now())
	v, err = mw.next.UnDoToDo(ctx, taskID)
	return
}

func (mw instrumentingMiddleware) DeleteToDo(ctx context.Context, taskID string) (v string, err error) {
	defer func(begin time.Time) {
		lvs := []string{"method", "DeleteToDo", "error", fmt.Sprint(err != nil)}
		mw.cubToDo.With(lvs...).Observe(time.Since(begin).Seconds())
	}(time.Now())
	v, err = mw.next.DeleteToDo(ctx, taskID)
	return
}

func (mw instrumentingMiddleware) GetAllToDo(ctx context.Context) (results []models.ToDoItem, err error) {
	defer func(begin time.Time) {
		lvs := []string{"method", "DeleteToDo",  "error", fmt.Sprint(err != nil)}
		mw.getToDo.With(lvs...).Observe(time.Since(begin).Seconds())
	}(time.Now())
	results, err = mw.next.GetAllToDo(ctx)
	return
}
