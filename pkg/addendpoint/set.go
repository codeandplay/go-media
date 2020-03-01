package addendpoint

import (
	"context"
	"time"

	"golang.org/x/time/rate"

	stdopentracing "github.com/opentracing/opentracing-go"
	stdzipkin "github.com/openzipkin/zipkin-go"
	"github.com/sony/gobreaker"

	"github.com/go-kit/kit/circuitbreaker"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/ratelimit"
	"github.com/go-kit/kit/tracing/opentracing"
	"github.com/go-kit/kit/tracing/zipkin"

	"ray.vhatt/todo-gokit/pkg/addservice"
	"ray.vhatt/todo-gokit/pkg/models"
)

// Set collects all of the endpoints that compose an add service. It's meant to
// be used as a helper struct, to collect all the endpoints into a single
// parameter.
type Set struct {
	SumEndpoint          endpoint.Endpoint
	ConcatEndpoint       endpoint.Endpoint
	PingEndpoint         endpoint.Endpoint
	AddToDoEndpoint      endpoint.Endpoint
	CompleteToDoEndPoint endpoint.Endpoint
	UnDoToDoEndpoint     endpoint.Endpoint
	DeleteToDoEndpoint   endpoint.Endpoint
	GetAllToDoEndpoint   endpoint.Endpoint
}

func New(svc addservice.Service, logger log.Logger, duration metrics.Histogram, otTracer stdopentracing.Tracer, zipkinTracer *stdzipkin.Tracer) Set {
	var sumEndpoint endpoint.Endpoint
	{
		sumEndpoint = MakeSumEndpoint(svc)
		// Sum is limited to 1 request per second with burst of 1 request.
		// Note, rate is defined as a time interval between requests.
		sumEndpoint = ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Every(time.Second), 1))(sumEndpoint)
		sumEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{}))(sumEndpoint)
		sumEndpoint = opentracing.TraceServer(otTracer, "Sum")(sumEndpoint)
		if zipkinTracer != nil {
			sumEndpoint = zipkin.TraceEndpoint(zipkinTracer, "Sum")(sumEndpoint)
		}
		sumEndpoint = LoggingMiddleware(log.With(logger, "method", "Sum"))(sumEndpoint)
		sumEndpoint = InstrumentingMiddleware(duration.With("method", "Sum"))(sumEndpoint)
	}
	var concatEndpoint endpoint.Endpoint
	{
		concatEndpoint = MakeConcatEndpoint(svc)
		// Concat is limited to 1 request per second with burst of 100 requests.
		// Note, rate is defined as a number of requests per second.
		concatEndpoint = ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Limit(1), 100))(concatEndpoint)
		concatEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{}))(concatEndpoint)
		concatEndpoint = opentracing.TraceServer(otTracer, "Concat")(concatEndpoint)
		if zipkinTracer != nil {
			concatEndpoint = zipkin.TraceEndpoint(zipkinTracer, "Concat")(concatEndpoint)
		}
		concatEndpoint = LoggingMiddleware(log.With(logger, "method", "Concat"))(concatEndpoint)
		concatEndpoint = InstrumentingMiddleware(duration.With("method", "Concat"))(concatEndpoint)
	}

	var pingEndpoint endpoint.Endpoint
	{
		pingEndpoint = MakePingEndpoint(svc)
		// Ping is limited to 1 request per second with burst of 100 requests.
		// Note, rate is defined as a number of requests per second.
		pingEndpoint = ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Limit(1), 100))(pingEndpoint)
		pingEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{}))(pingEndpoint)
		pingEndpoint = opentracing.TraceServer(otTracer, "Ping")(pingEndpoint)
		if zipkinTracer != nil {
			concatEndpoint = zipkin.TraceEndpoint(zipkinTracer, "Ping")(pingEndpoint)
		}
		pingEndpoint = LoggingMiddleware(log.With(logger, "method", "Ping"))(pingEndpoint)
		pingEndpoint = InstrumentingMiddleware(duration.With("method", "Ping"))(pingEndpoint)
	}

	var addToDoEndpoint endpoint.Endpoint
	{
		addToDoEndpoint = MakeAddToDoEndpoint(svc)
		// AddToDo is limited to 1 request per second with burst of 100 requests.
		// Note, rate is defined as a number of requests per second.
		addToDoEndpoint = ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Limit(1), 100))(addToDoEndpoint)
		addToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{}))(addToDoEndpoint)
		addToDoEndpoint = opentracing.TraceServer(otTracer, "AddToDo")(addToDoEndpoint)
		if zipkinTracer != nil {
			addToDoEndpoint = zipkin.TraceEndpoint(zipkinTracer, "AddToDo")(addToDoEndpoint)
		}
		addToDoEndpoint = LoggingMiddleware(log.With(logger, "method", "AddToDo"))(addToDoEndpoint)
		addToDoEndpoint = InstrumentingMiddleware(duration.With("method", "AddToDo"))(addToDoEndpoint)
	}

	var completeToDoEndpoint endpoint.Endpoint
	{
		completeToDoEndpoint = MakeCompleteToDoEndpoint(svc)
		// CompletToDo is limited to 1 request per second with burst of 100 requests.
		// Note, rate is defined as a number of requests per second.
		completeToDoEndpoint = ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Limit(1), 100))(completeToDoEndpoint)
		completeToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{}))(completeToDoEndpoint)
		completeToDoEndpoint = opentracing.TraceServer(otTracer, "CompleteToDo")(completeToDoEndpoint)
		if zipkinTracer != nil {
			completeToDoEndpoint = zipkin.TraceEndpoint(zipkinTracer, "CompleteToDo")(completeToDoEndpoint)
		}
		completeToDoEndpoint = LoggingMiddleware(log.With(logger, "method", "CompleteToDo"))(completeToDoEndpoint)
		completeToDoEndpoint = InstrumentingMiddleware(duration.With("method", "CompleteToDo"))(completeToDoEndpoint)
	}

	var unDoToDoEndpoint endpoint.Endpoint
	{
		unDoToDoEndpoint = MakeUnDoToDoEndpoint(svc)
		// unDoToDo is limited to 1 request per second with burst of 100 requests.
		// Note, rate is defined as a number of requests per second.
		unDoToDoEndpoint = ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Limit(1), 100))(unDoToDoEndpoint)
		unDoToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{}))(unDoToDoEndpoint)
		unDoToDoEndpoint = opentracing.TraceServer(otTracer, "UndoToDo")(unDoToDoEndpoint)
		if zipkinTracer != nil {
			unDoToDoEndpoint = zipkin.TraceEndpoint(zipkinTracer, "UndoToDo")(unDoToDoEndpoint)
		}
		unDoToDoEndpoint = LoggingMiddleware(log.With(logger, "method", "UnDoToDo"))(unDoToDoEndpoint)
		unDoToDoEndpoint = InstrumentingMiddleware(duration.With("method", "UnDoToDo"))(unDoToDoEndpoint)
	}

	var deleteToDoEndpoint endpoint.Endpoint
	{
		deleteToDoEndpoint = MakeDeleteToDoEndpoint(svc)
		// deleteToDo is limited to 1 request per second with burst of 100 requests.
		// Note, rate is defined as a number of requests per second.
		deleteToDoEndpoint = ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Limit(1), 100))(deleteToDoEndpoint)
		deleteToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{}))(deleteToDoEndpoint)
		deleteToDoEndpoint = opentracing.TraceServer(otTracer, "DeleteToDo")(deleteToDoEndpoint)
		if zipkinTracer != nil {
			deleteToDoEndpoint = zipkin.TraceEndpoint(zipkinTracer, "DeleteToDo")(deleteToDoEndpoint)
		}
		deleteToDoEndpoint = LoggingMiddleware(log.With(logger, "method", "DeleteToDo"))(deleteToDoEndpoint)
		deleteToDoEndpoint = InstrumentingMiddleware(duration.With("method", "DeleteToDo"))(deleteToDoEndpoint)
	}

	var getAllToDoEndpoint endpoint.Endpoint
	{
		getAllToDoEndpoint = MakeGetAllToDoEndpoint(svc)
		// getAllToDo is limited to 1 request per second with burst of 100 requests.
		// Note, rate is defined as a number of requests per second.
		getAllToDoEndpoint = ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Limit(1), 100))(getAllToDoEndpoint)
		getAllToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{}))(getAllToDoEndpoint)
		getAllToDoEndpoint = opentracing.TraceServer(otTracer, "GetAllToDo")(getAllToDoEndpoint)
		if zipkinTracer != nil {
			getAllToDoEndpoint = zipkin.TraceEndpoint(zipkinTracer, "GetAllToDo")(getAllToDoEndpoint)
		}
		getAllToDoEndpoint = LoggingMiddleware(log.With(logger, "method", "GetAllToDo"))(getAllToDoEndpoint)
		getAllToDoEndpoint = InstrumentingMiddleware(duration.With("method", "GetAllToDo"))(getAllToDoEndpoint)
	}

	return Set{
		SumEndpoint:          sumEndpoint,
		ConcatEndpoint:       concatEndpoint,
		PingEndpoint:         pingEndpoint,
		AddToDoEndpoint:      addToDoEndpoint,
		CompleteToDoEndPoint: completeToDoEndpoint,
		UnDoToDoEndpoint:     unDoToDoEndpoint,
		DeleteToDoEndpoint:   deleteToDoEndpoint,
		GetAllToDoEndpoint:   getAllToDoEndpoint,
	}
}

// Sum implements the service interface, so Set maybe used as a service.
// This is primarily usefule in the context of a client library.
func (s Set) Sum(ctx context.Context, a, b int) (int, error) {
	resp, err := s.SumEndpoint(ctx, SumRequest{A: a, B: b})
	if err != nil {
		return 0, err
	}

	response := resp.(SumResponse)
	return response.V, response.Err
}

// Concat implements the service interface, so Set may be used a
// service. This is primarily useful in the context of a client library.
func (s Set) Concat(ctx context.Context, a, b string) (string, error) {
	resp, err := s.ConcatEndpoint(ctx, ConcatRequest{A: a, B: b})
	if err != nil {
		return "", err
	}

	response := resp.(ConcatResponse)
	return response.V, response.Err
}

// Ping implements the service interface, so Set may be used a
// service. This is primarily useful in the context of a client library.
func (s Set) Ping(ctx context.Context) (string, error) {
	resp, err := s.PingEndpoint(ctx, PingRequest{})
	if err != nil {
		return "", err
	}

	response := resp.(PingResponse)
	return response.V, response.Err
}

// AddToDo implements the service interface, so Set may be used a
// service. This is primarily useful in the context of a client library.
func (s Set) AddToDo(ctx context.Context, task models.ToDoItem) (string, error) {
	resp, err := s.AddToDoEndpoint(ctx, AddToDoRequest(task))
	if err != nil {
		return "", err
	}

	response := resp.(AddToDoResponse)
	return response.TaskID, response.Err
}

// CompleteToDo implements the service interface, so Set may be used a
// service. This is primarily useful in the context of a client library.
func (s Set) CompleteToDo(ctx context.Context, taskID string) (string, error) {
	resp, err := s.CompleteToDoEndPoint(ctx, CompleteToDoRequest{TaskID: taskID})
	if err != nil {
		return "", err
	}

	response := resp.(CompleteToDoResponse)
	return response.TaskID, response.Err
}

// UndoToDo implements the service interface, so Set may be used a
// service. This is primarily useful in the context of a client library.
func (s Set) UnDoToDo(ctx context.Context, taskID string) (string, error) {
	resp, err := s.UnDoToDoEndpoint(ctx, UnDoToDoRequest{TaskID: taskID})
	if err != nil {
		return "", err
	}

	response := resp.(UnDoToDoResponse)
	return response.TaskID, response.Err
}

// DeleteToDo implements the service interface, so Set may be used a
// service. This is primarily useful in the context of a client library.
func (s Set) DeleteToDo(ctx context.Context, taskID string) (string, error) {
	resp, err := s.DeleteToDoEndpoint(ctx, DeleteToDoRequest{TaskID: taskID})
	if err != nil {
		return "", err
	}

	response := resp.(DeleteToDoResponse)
	return response.TaskID, response.Err
}

// GetAllToDo implements the service interface, so Set may be used a
// service. This is primarily useful in the context of a client library.
func (s Set) GetAllToDo(ctx context.Context) ([]models.ToDoItem, error) {
	resp, err := s.GetAllToDoEndpoint(ctx, GetAllToDoRequest{})
	if err != nil {
		return nil, err
	}

	response := resp.(GetAllToDoResponse)
	return response.Todos, response.Err
}

// MakeSumEndpoint constructs a Sum endpoint wrapping the service.
func MakeSumEndpoint(s addservice.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(SumRequest)
		v, err := s.Sum(ctx, req.A, req.B)
		return SumResponse{V: v, Err: err}, nil
	}
}

// MakeConcatEndpoint constructs a Concat endpoint wrapping the service.
func MakeConcatEndpoint(s addservice.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(ConcatRequest)
		v, err := s.Concat(ctx, req.A, req.B)
		return ConcatResponse{V: v, Err: err}, nil
	}
}

// MakePingEndpoint constructs a Ping endpoint wrapping the service.
func MakePingEndpoint(s addservice.Service) endpoint.Endpoint {
	return func(ctx context.Context, _ interface{}) (response interface{}, err error) {
		v, err := s.Ping(ctx)
		return ConcatResponse{V: v, Err: err}, nil
	}
}

// MakeAddToDoEndpoint constructs a AddToDo endpoint wrapping the service.
func MakeAddToDoEndpoint(s addservice.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(AddToDoRequest)
		v, err := s.AddToDo(ctx, req)
		return AddToDoResponse{TaskID: v, Err: err}, nil
	}
}

// MakeCompleteToDoEndpoint constructs a CompleteToDo endpoint wrapping the service.
func MakeCompleteToDoEndpoint(s addservice.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(CompleteToDoRequest)
		v, err := s.CompleteToDo(ctx, req.TaskID)
		return CompleteToDoResponse{TaskID: v, Err: err}, nil
	}
}

// MakeUnDoToDoEndpoint constructs a UnDoToDo endpoint wrapping the service.
func MakeUnDoToDoEndpoint(s addservice.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(UnDoToDoRequest)
		v, err := s.UnDoToDo(ctx, req.TaskID)
		return UnDoToDoResponse{TaskID: v, Err: err}, nil
	}
}

// MakeDeleteToDoEndpoint constructs a DeleteToDo endpoint wrapping the service.
func MakeDeleteToDoEndpoint(s addservice.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(DeleteToDoRequest)
		v, err := s.DeleteToDo(ctx, req.TaskID)
		return DeleteToDoResponse{TaskID: v, Err: err}, nil
	}
}

// MakeGetAllToDoEndpoint constructs a GetAllToDo endpoint wrapping the service.
func MakeGetAllToDoEndpoint(s addservice.Service) endpoint.Endpoint {
	return func(ctx context.Context, _ interface{}) (response interface{}, err error) {
		v, err := s.GetAllToDo(ctx)
		return GetAllToDoResponse{Todos: v, Err: err}, nil
	}
}

// compile time assertions for our response types implements endpoint.Failer.
var (
	_ endpoint.Failer = SumResponse{}
	_ endpoint.Failer = ConcatResponse{}
	_ endpoint.Failer = PingResponse{}
	_ endpoint.Failer = AddToDoResponse{}
	_ endpoint.Failer = CompleteToDoResponse{}
	_ endpoint.Failer = UnDoToDoResponse{}
	_ endpoint.Failer = DeleteToDoResponse{}
	_ endpoint.Failer = GetAllToDoResponse{}
)

// SumRequest collects the request parameters for the Sum method.
type SumRequest struct {
	A, B int
}

// SumResponse collects the response values for teh Sum method.
type SumResponse struct {
	V   int   `json:"v"`
	Err error `json:"-"` // should be intercepted by Failed/errorEncoder
}

// Failed implements endpoint.Failer.
func (r SumResponse) Failed() error { return r.Err }

// ConcatRequest collects the request parameters for the Concat method.
type ConcatRequest struct {
	A, B string
}

// ConcatResponse collects the response values for the Concat method.
type ConcatResponse struct {
	V   string `json:"v"`
	Err error  `json:"-"` // shoudl be intercepted by Failed/erroEncoder
}

// Failed implements endpoint.Failer.
func (r ConcatResponse) Failed() error { return r.Err }

// PingRequest collects the request parameters for the Concat method.
type PingRequest struct {
}

// PingResponse collects the response values for the Concat method.
type PingResponse struct {
	V   string `json:"v"`
	Err error  `json:"-"` // shoudl be intercepted by Failed/erroEncoder
}

// Failed implements endpoint.Failer.
func (r PingResponse) Failed() error { return r.Err }

// AddToDo collect request parameters for the AddTodo method
type AddToDoRequest = models.ToDoItem

// AddToDoResponse collects the response values for the AddToDo method.
type AddToDoResponse struct {
	TaskID string `json:"taskID"`
	Err    error  `json:"-"` // should be intercepted by Failed/errEncoder
}

// Failed implements endpoint.Failer.
func (r AddToDoResponse) Failed() error { return r.Err }

// CompleteToDoRequest collect request parameters for the CompleteToDo method
type CompleteToDoRequest struct {
	TaskID string `json:"taskID"`
}

// CompleteToDoResponse collects the response values for the CompleteToDo method.
type CompleteToDoResponse struct {
	TaskID string `json:"taskID"`
	Err    error  `json:"-"` // should be intercepted by Failed/errEncoder
}

// Failed implements endpoint.Failer.
func (r CompleteToDoResponse) Failed() error { return r.Err }

// UnDoToDoRequest collect request parameters for the UnDoToDoRequest method
type UnDoToDoRequest struct {
	TaskID string `json:"taskID"`
}

// UnDoToDoResponse collects the response values for the UnDoToDoResponse method.
type UnDoToDoResponse struct {
	TaskID string `json:"taskID"`
	Err    error  `json:"-"` // should be intercepted by Failed/errEncoder
}

// Failed implements endpoint.Failer.
func (r UnDoToDoResponse) Failed() error { return r.Err }

// DeleteDoRequest collect request parameters for the DeleteDoRequest method
type DeleteToDoRequest struct {
	TaskID string `json:"taskID"`
}

// DeleteToDoResponse collects the response values for the DeleteToDoResponse method.
type DeleteToDoResponse struct {
	TaskID string `json:"taskID"`
	Err    error  `json:"-"` // should be intercepted by Failed/errEncoder
}

// Failed implements endpoint.Failer.
func (r DeleteToDoResponse) Failed() error { return r.Err }

// GetAllToDoRequest collect request parameters for the GetAllToDoRequest method
type GetAllToDoRequest struct{}

// GetAllToDoResponse collects the response values for the GetAllToDoResponse method.
type GetAllToDoResponse struct {
	Todos []models.ToDoItem `json:"todos"`
	Err   error             `json:"-"` // should be intercepted by Failed/errEncoder
}

// Failed implements endpoint.Failer.
func (r GetAllToDoResponse) Failed() error { return r.Err }
