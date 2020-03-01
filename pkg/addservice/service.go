package addservice

import (
	"context"
	"errors"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"

	"ray.vhatt/todo-gokit/pkg/models"
	"ray.vhatt/todo-gokit/pkg/store"
)

// Service describe a service that adds things together
type Service interface {
	Sum(ctx context.Context, a, b int) (int, error)
	Concat(ctx context.Context, a, b string) (string, error)
	Ping(ctx context.Context) (string, error)
	AddToDo(ctx context.Context, task models.ToDoItem) (string, error)
	CompleteToDo(ctx context.Context, taskId string) (string, error)
	UnDoToDo(ctx context.Context, taskId string) (string, error)
	DeleteToDo(ctx context.Context, taskId string) (string, error)
	GetAllToDo(ctx context.Context) ([]models.ToDoItem, error)
}

// New return a basic Service with all the expected middlewares wired in.
func New(logger log.Logger, ints, chars metrics.Counter, cubTodo, getTodo metrics.Histogram) Service {
	var svc Service
	{
		svc = NewBasicService()
		svc = LoggingMiddleware(logger)(svc)
		svc = InstrumentingMiddleware(ints, chars, cubTodo, getTodo)(svc)
	}

	return svc
}

var (
	// ErrTwoZeroes is an arbitrary business rule for the Add method.
	ErrTwoZeroes = errors.New("can't sum two zeroes")

	// ErrIntOverflow protects the Add method. We've decided that this error
	// indicateds a misbehaving service and should count against e.g. circuit
	// breakers. So, we return it directlly in endpoints, to illustrate the
	// difference. In a real service, this probably wouldn't be the case.
	ErrIntOverflow = errors.New("integer overflow")

	// ErrMaxSizeExceeded protects the Concat method.
	ErrMaxSizeExceeded = errors.New("result exceeds maximum size")
)

// NewBasicService return a naive, stateless implementation of Service.
func NewBasicService() Service {
	dbStore, _ := store.NewMongoStore("mongodb://localhost:27017", "gokit-test", "todolist")
	return basicService{
		dbStore: dbStore,
	}
}

type basicService struct {
	dbStore store.Store
}

const (
	intMax = 1<<31 - 1
	intMin = -(intMax + 1)
	maxLen = 10
)

// Sum implements Sum
func (s basicService) Sum(_ context.Context, a, b int) (int, error) {
	if a == 0 && b == 0 {
		return 0, ErrTwoZeroes
	}

	if (b > 0 && a > (intMax-b)) || (b < 0 && a < (intMin-b)) {
		return 0, ErrIntOverflow
	}

	return a + b, nil
}

func (s basicService) Concat(_ context.Context, a, b string) (string, error) {
	if len(a)+len(b) > maxLen {
		return "", ErrMaxSizeExceeded
	}
	return a + b, nil
}

func (s basicService) Ping(ctx context.Context) (string, error) {
	err := s.dbStore.Ping(ctx)
	if err != nil {
		return "down", nil
	}
	return "up", nil
}

func (s basicService) AddToDo(ctx context.Context, task models.ToDoItem) (string, error) {
	insertResult, err := s.dbStore.InsertToDo(ctx, task)
	if err != nil {
		return "", err
	}
	return insertResult, nil
}

func (s basicService) CompleteToDo(ctx context.Context, taskID string) (string, error) {
	resultID, err := s.dbStore.CompleteToDo(ctx, taskID)
	if err != nil {
		return "", err
	}

	return resultID, nil
}

func (s basicService) UnDoToDo(ctx context.Context, taskID string) (string, error) {
	resultID, err := s.dbStore.UnDoToDo(ctx, taskID)
	if err != nil {
		return "", err
	}

	return resultID, nil
}

func (s basicService) DeleteToDo(ctx context.Context, taskID string) (string, error) {
	resultID, err := s.dbStore.DeleteToDo(ctx, taskID)
	if err != nil {
		return "", err
	}

	return resultID, nil
}

func (s basicService) GetAllToDo(ctx context.Context) ([]models.ToDoItem, error) {
	results, err := s.dbStore.GetAllToDo(ctx)
	if err != nil {
		return nil, err
	}
	return results, nil
}
