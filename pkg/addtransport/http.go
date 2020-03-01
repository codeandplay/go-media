package addtransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/time/rate"

	stdopentracing "github.com/opentracing/opentracing-go"
	stdzipkin "github.com/openzipkin/zipkin-go"
	"github.com/sony/gobreaker"

	"github.com/go-kit/kit/circuitbreaker"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/ratelimit"
	"github.com/go-kit/kit/tracing/opentracing"
	"github.com/go-kit/kit/tracing/zipkin"
	"github.com/go-kit/kit/transport"
	httptransport "github.com/go-kit/kit/transport/http"

	"ray.vhatt/todo-gokit/pkg/addendpoint"
	"ray.vhatt/todo-gokit/pkg/addservice"
)

// NewHTTPHandler returns an HTTP handler that makes a set of endpoints
// available on predefined paths.
func NewHTTPHandler(endpoints addendpoint.Set, otTracer stdopentracing.Tracer, zipkinTracer *stdzipkin.Tracer, logger log.Logger) http.Handler {
	options := []httptransport.ServerOption{
		httptransport.ServerErrorEncoder(errorEncoder),
		httptransport.ServerErrorHandler(transport.NewLogErrorHandler(logger)),
	}

	if zipkinTracer != nil {
		// Zipkin HTTP Server Trace can either be instantiated per endpoint with a
		// provided operation name or a global tracing service can be instantiated
		// without an operation name and fed to each Go kit endpoint as ServerOption.
		// In the latter case, the operation name will be the endpoint's http method.
		// We demonstrate a global tracing service here.
		options = append(options, zipkin.HTTPServerTrace(zipkinTracer))
	}

	m := http.NewServeMux()
	m.Handle("/sum", httptransport.NewServer(
		endpoints.SumEndpoint,
		decodeHTTPSumRequest,
		encodeHTTPGenericResponse,
		append(options, httptransport.ServerBefore(opentracing.HTTPToContext(otTracer, "Sum", logger)))...,
	))
	m.Handle("/concat", httptransport.NewServer(
		endpoints.ConcatEndpoint,
		decodeHTTPConcatRequest,
		encodeHTTPGenericResponse,
		append(options, httptransport.ServerBefore(opentracing.HTTPToContext(otTracer, "Concat", logger)))...,
	))

	m.Handle("/ping", httptransport.NewServer(
		endpoints.PingEndpoint,
		decodeHTTPPingRequest,
		encodeHTTPGenericResponse,
		append(options, httptransport.ServerBefore(opentracing.HTTPToContext(otTracer, "Ping", logger)))...,
	))

	m.Handle("/addToDo", httptransport.NewServer(
		endpoints.AddToDoEndpoint,
		decodeHTTPAddToDoRequest,
		encodeHTTPGenericResponse,
		append(options, httptransport.ServerBefore(opentracing.HTTPToContext(otTracer, "AddToDo", logger)))...,
	))

	m.Handle("/completeToDo", httptransport.NewServer(
		endpoints.CompleteToDoEndPoint,
		decodeHTTPCompleteToDoRequest,
		encodeHTTPGenericResponse,
		append(options, httptransport.ServerBefore(opentracing.HTTPToContext(otTracer, "CompleteToDo", logger)))...,
	))

	m.Handle("/unDoToDo", httptransport.NewServer(
		endpoints.UnDoToDoEndpoint,
		decodeHTTPUnDoToDoRequest,
		encodeHTTPGenericResponse,
		append(options, httptransport.ServerBefore(opentracing.HTTPToContext(otTracer, "UnDoToDo", logger)))...,
	))

	m.Handle("/deleteToDo", httptransport.NewServer(
		endpoints.DeleteToDoEndpoint,
		decodeHTTPDeleteToDoRequest,
		encodeHTTPGenericResponse,
		append(options, httptransport.ServerBefore(opentracing.HTTPToContext(otTracer, "DeleteToDo", logger)))...,
	))

	m.Handle("/getAllToDo", httptransport.NewServer(
		endpoints.GetAllToDoEndpoint,
		decodeHTTPGetAllToDoRequest,
		encodeHTTPGenericResponse,
		append(options, httptransport.ServerBefore(opentracing.HTTPToContext(otTracer, "GetAllToDo", logger)))...,
	))

	return m
}

// NewHTTPClient returns an AddService backed by an HTTP server living at the
// remote instance. We expect instance to come from a service discovery system,
// so likely of the form "host:port". We bake-in certain middlewares,
// implementing the client library pattern.
func NewHTTPClient(instance string, otTracer stdopentracing.Tracer, zipkinTracer *stdzipkin.Tracer, logger log.Logger) (addservice.Service, error) {
	// Quickly sanitize the instance string.
	if !strings.HasPrefix(instance, "http") {
		instance = "http://" + instance
	}
	u, err := url.Parse(instance)
	if err != nil {
		return nil, err
	}

	// We construct a single ratelimiter middleware, to limit the total outgoing
	// QPS from this client to all methods on the remote instance. We also
	// construct per-endpoint circuitbreaker middlewares to demonstrate how
	// that's done, although they could easily be combined into a single breaker
	// for the entire remote instance, too.
	limiter := ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Every(time.Second), 100))

	// global client middlewares
	var options []httptransport.ClientOption

	if zipkinTracer != nil {
		// Zipkin HTTP Client Trace can either be instantiated per endpoint with a
		// provided operation name or a global tracing client can be instantiated
		// without an operation name and fed to each Go kit endpoint as ClientOption.
		// In the latter case, the operation name will be the endpoint's http method.
		options = append(options, zipkin.HTTPClientTrace(zipkinTracer))
	}

	// Each individual endpoint is an http/transport.Client (which implements
	// endpoint.Endpoint) that gets wrapped with various middlewares. If you
	// made your own client library, you'd do this work there, so your server
	// could rely on a consistent set of client behavior.
	var sumEndpoint endpoint.Endpoint
	{
		sumEndpoint = httptransport.NewClient(
			"POST",
			copyURL(u, "/sum"),
			encodeHTTPGenericRequest,
			decodeHTTPSumResponse,
			append(options, httptransport.ClientBefore(opentracing.ContextToHTTP(otTracer, logger)))...,
		).Endpoint()
		sumEndpoint = opentracing.TraceClient(otTracer, "Sum")(sumEndpoint)
		if zipkinTracer != nil {
			sumEndpoint = zipkin.TraceEndpoint(zipkinTracer, "Sum")(sumEndpoint)
		}
		sumEndpoint = limiter(sumEndpoint)
		sumEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "Sum",
			Timeout: 30 * time.Second,
		}))(sumEndpoint)
	}

	// The Concat endpoint is the same thing, with slightly different
	// middlewares to demonstrate how to specialize per-endpoint.
	var concatEndpoint endpoint.Endpoint
	{
		concatEndpoint = httptransport.NewClient(
			"POST",
			copyURL(u, "/concat"),
			encodeHTTPGenericRequest,
			decodeHTTPConcatResponse,
			append(options, httptransport.ClientBefore(opentracing.ContextToHTTP(otTracer, logger)))...,
		).Endpoint()
		concatEndpoint = opentracing.TraceClient(otTracer, "Concat")(concatEndpoint)
		if zipkinTracer != nil {
			concatEndpoint = zipkin.TraceEndpoint(zipkinTracer, "Concat")(concatEndpoint)
		}
		concatEndpoint = limiter(concatEndpoint)
		concatEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "Concat",
			Timeout: 10 * time.Second,
		}))(concatEndpoint)
	}

	// The Ping endpoint is the same thing, with slightly different
	// middlewares to demonstrate how to specialize per-endpoint.
	var pingEndpoint endpoint.Endpoint
	{
		pingEndpoint = httptransport.NewClient(
			"GET",
			copyURL(u, "/ping"),
			encodeHTTPGenericRequest,
			decodeHTTPPingResponse,
			append(options, httptransport.ClientBefore(opentracing.ContextToHTTP(otTracer, logger)))...,
		).Endpoint()
		pingEndpoint = opentracing.TraceClient(otTracer, "Ping")(pingEndpoint)
		if zipkinTracer != nil {
			pingEndpoint = zipkin.TraceEndpoint(zipkinTracer, "Ping")(pingEndpoint)
		}
		pingEndpoint = limiter(pingEndpoint)
		pingEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "Ping",
			Timeout: 10 * time.Second,
		}))(pingEndpoint)
	}

	// The AddToDo endpoint is the same thing, with slightly different
	// middlewares to demonstrate how to specialize per-endpoint.
	var addToDoEndpoint endpoint.Endpoint
	{
		addToDoEndpoint = httptransport.NewClient(
			"POST",
			copyURL(u, "/addToDo"),
			encodeHTTPGenericRequest,
			decodeHTTPAddToDoResponse,
			append(options, httptransport.ClientBefore(opentracing.ContextToHTTP(otTracer, logger)))...,
		).Endpoint()
		addToDoEndpoint = opentracing.TraceClient(otTracer, "AddToDo")(pingEndpoint)
		if zipkinTracer != nil {
			pingEndpoint = zipkin.TraceEndpoint(zipkinTracer, "AddToDo")(pingEndpoint)
		}
		addToDoEndpoint = limiter(addToDoEndpoint)
		addToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "AddToDo",
			Timeout: 10 * time.Second,
		}))(addToDoEndpoint)
	}

	// The CompleteToDo endpoint is the same thing, with slightly different
	// middlewares to demonstrate how to specialize per-endpoint.
	var completeToDoEndpoint endpoint.Endpoint
	{
		completeToDoEndpoint = httptransport.NewClient(
			"PUT",
			copyURL(u, "/completeToDo"),
			encodeHTTPGenericRequest,
			decodeHTTPCompleteToDoResponse,
			append(options, httptransport.ClientBefore(opentracing.ContextToHTTP(otTracer, logger)))...,
		).Endpoint()
		completeToDoEndpoint = opentracing.TraceClient(otTracer, "CompleteToDo")(pingEndpoint)
		if zipkinTracer != nil {
			pingEndpoint = zipkin.TraceEndpoint(zipkinTracer, "CompleteToDo")(pingEndpoint)
		}
		completeToDoEndpoint = limiter(completeToDoEndpoint)
		completeToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "CompleteToDo",
			Timeout: 10 * time.Second,
		}))(completeToDoEndpoint)
	}

	// The UnDoToDo endpoint is the same thing, with slightly different
	// middlewares to demonstrate how to specialize per-endpoint.
	var unDoToDoEndpoint endpoint.Endpoint
	{
		unDoToDoEndpoint = httptransport.NewClient(
			"PUT",
			copyURL(u, "/unDoToDo"),
			encodeHTTPGenericRequest,
			decodeHTTPUnDoToDoResponse,
			append(options, httptransport.ClientBefore(opentracing.ContextToHTTP(otTracer, logger)))...,
		).Endpoint()
		unDoToDoEndpoint = opentracing.TraceClient(otTracer, "UnDoToDo")(pingEndpoint)
		if zipkinTracer != nil {
			pingEndpoint = zipkin.TraceEndpoint(zipkinTracer, "UnDoToDo")(pingEndpoint)
		}
		unDoToDoEndpoint = limiter(unDoToDoEndpoint)
		unDoToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "UnDoToDo",
			Timeout: 10 * time.Second,
		}))(unDoToDoEndpoint)
	}

	// The DeleteToDo endpoint is the same thing, with slightly different
	// middlewares to demonstrate how to specialize per-endpoint.
	var deleteToDoEndpoint endpoint.Endpoint
	{
		deleteToDoEndpoint = httptransport.NewClient(
			"DELETE",
			copyURL(u, "/deleteToDo"),
			encodeHTTPGenericRequest,
			decodeHTTPDeleteToDoResponse,
			append(options, httptransport.ClientBefore(opentracing.ContextToHTTP(otTracer, logger)))...,
		).Endpoint()
		deleteToDoEndpoint = opentracing.TraceClient(otTracer, "DeleteToDo")(pingEndpoint)
		if zipkinTracer != nil {
			pingEndpoint = zipkin.TraceEndpoint(zipkinTracer, "DeleteToDo")(pingEndpoint)
		}
		deleteToDoEndpoint = limiter(deleteToDoEndpoint)
		deleteToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "DeleteToDo",
			Timeout: 10 * time.Second,
		}))(deleteToDoEndpoint)
	}

	// The GetAllToDo endpoint is the same thing, with slightly different
	// middlewares to demonstrate how to specialize per-endpoint.
	var getAllToDoEndpoint endpoint.Endpoint
	{
		getAllToDoEndpoint = httptransport.NewClient(
			"GET",
			copyURL(u, "/getAllToDo"),
			encodeHTTPGenericRequest,
			decodeHTTPGetAllToDoResponse,
			append(options, httptransport.ClientBefore(opentracing.ContextToHTTP(otTracer, logger)))...,
		).Endpoint()
		getAllToDoEndpoint = opentracing.TraceClient(otTracer, "GetAllToDo")(pingEndpoint)
		if zipkinTracer != nil {
			pingEndpoint = zipkin.TraceEndpoint(zipkinTracer, "GetAllToDo")(pingEndpoint)
		}
		getAllToDoEndpoint = limiter(deleteToDoEndpoint)
		getAllToDoEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "GetAllToDo",
			Timeout: 10 * time.Second,
		}))(getAllToDoEndpoint)
	}

	// Returning the endpoint.Set as a service.Service relies on the
	// endpoint.Set implementing the Service methods. That's just a simple bit
	// of glue code.
	return addendpoint.Set{
		SumEndpoint:          sumEndpoint,
		ConcatEndpoint:       concatEndpoint,
		PingEndpoint:         pingEndpoint,
		AddToDoEndpoint:      addToDoEndpoint,
		CompleteToDoEndPoint: completeToDoEndpoint,
		UnDoToDoEndpoint:     unDoToDoEndpoint,
		DeleteToDoEndpoint:   deleteToDoEndpoint,
		GetAllToDoEndpoint:   getAllToDoEndpoint,
	}, nil
}

func copyURL(base *url.URL, path string) *url.URL {
	next := *base
	next.Path = path
	return &next
}

func errorEncoder(_ context.Context, err error, w http.ResponseWriter) {
	w.WriteHeader(err2code(err))
	json.NewEncoder(w).Encode(errorWrapper{Error: err.Error()})
}

func err2code(err error) int {
	switch err {
	case addservice.ErrTwoZeroes, addservice.ErrMaxSizeExceeded, addservice.ErrIntOverflow:
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func errorDecoder(r *http.Response) error {
	var w errorWrapper
	if err := json.NewDecoder(r.Body).Decode(&w); err != nil {
		return err
	}
	return errors.New(w.Error)
}

type errorWrapper struct {
	Error string `json:"error"`
}

// decodeHTTPSumRequest is a transport/http.DecodeRequestFunc that decodes a
// JSON-encoded sum request from the HTTP request body. Primarily useful in a
// server.
func decodeHTTPSumRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req addendpoint.SumRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

// decodeHTTPConcatRequest is a transport/http.DecodeRequestFunc that decodes a
// JSON-encoded concat request from the HTTP request body. Primarily useful in a
// server.
func decodeHTTPConcatRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req addendpoint.ConcatRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

// decodeHTTPPingRequest is a transport/http.DecodeRequestFunc that decodes a
// JSON-encoded ping request from the HTTP request body. Primarily useful in a
// server.
func decodeHTTPPingRequest(_ context.Context, r *http.Request) (interface{}, error) {
	return addendpoint.PingRequest{}, nil
}

// decodeHTTPAddToDoRequest is a transport/http.DecodeRequestFunc that decodes a
// JSON-encoded addToDo request from the HTTP request body. Primarily useful in a
// server.
func decodeHTTPAddToDoRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req addendpoint.AddToDoRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

// decodeHTTPCompleteToDoRequest is a transport/http.DecodeRequestFunc that decodes a
// JSON-encoded completeToDo request from the HTTP request body. Primarily useful in a
// server.
func decodeHTTPCompleteToDoRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req addendpoint.CompleteToDoRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

// decodeHTTPUnDoToDoRequest is a transport/http.DecodeRequestFunc that decodes a
// JSON-encoded unDoToDo request from the HTTP request body. Primarily useful in a
// server.
func decodeHTTPUnDoToDoRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req addendpoint.UnDoToDoRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

// decodeHTTPDeleteToDoRequest is a transport/http.DecodeRequestFunc that decodes a
// JSON-encoded deleteToDo request from the HTTP request body. Primarily useful in a
// server.
func decodeHTTPDeleteToDoRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req addendpoint.DeleteToDoRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

// decodeHTTPGetAllToDoRequest is a transport/http.DecodeRequestFunc that decodes a
// JSON-encoded getAllToDo request from the HTTP request body. Primarily useful in a
// server.
func decodeHTTPGetAllToDoRequest(_ context.Context, r *http.Request) (interface{}, error) {
	return addendpoint.GetAllToDoRequest{}, nil
}

// decodeHTTPSumResponse is a transport/http.DecodeResponseFunc that decodes a
// JSON-encoded sum response from the HTTP response body. If the response has a
// non-200 status code, we will interpret that as an error and attempt to decode
// the specific error message from the response body. Primarily useful in a
// client.
func decodeHTTPSumResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.SumResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

// decodeHTTPConcatResponse is a transport/http.DecodeResponseFunc that decodes
// a JSON-encoded concat response from the HTTP response body. If the response
// has a non-200 status code, we will interpret that as an error and attempt to
// decode the specific error message from the response body. Primarily useful in
// a client.
func decodeHTTPConcatResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.ConcatResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

// decodeHTTPPingResponse is a transport/http.DecodeResponseFunc that decodes
// a JSON-encoded concat response from the HTTP response body. If the response
// has a non-200 status code, we will interpret that as an error and attempt to
// decode the specific error message from the response body. Primarily useful in
// a client.
func decodeHTTPPingResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.PingResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

// decodeHTTPAddToDoResponse is a transport/http.DecodeResponseFunc that decodes
// a JSON-encoded concat response from the HTTP response body. If the response
// has a non-200 status code, we will interpret that as an error and attempt to
// decode the specific error message from the response body. Primarily useful in
// a client.
func decodeHTTPAddToDoResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.AddToDoResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

// decodeHTTPCompleteToDoResponse is a transport/http.DecodeResponseFunc that decodes
// a JSON-encoded concat response from the HTTP response body. If the response
// has a non-200 status code, we will interpret that as an error and attempt to
// decode the specific error message from the response body. Primarily useful in
// a client.
func decodeHTTPCompleteToDoResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.CompleteToDoResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

// decodeHTTPUnDoToDoResponse is a transport/http.DecodeResponseFunc that decodes
// a JSON-encoded concat response from the HTTP response body. If the response
// has a non-200 status code, we will interpret that as an error and attempt to
// decode the specific error message from the response body. Primarily useful in
// a client.
func decodeHTTPUnDoToDoResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.UnDoToDoResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

// decodeHTTPDeleteToDoResponse is a transport/http.DecodeResponseFunc that decodes
// a JSON-encoded concat response from the HTTP response body. If the response
// has a non-200 status code, we will interpret that as an error and attempt to
// decode the specific error message from the response body. Primarily useful in
// a client.
func decodeHTTPDeleteToDoResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.DeleteToDoResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

// decodeHTTPGetAllToDoResponse is a transport/http.DecodeResponseFunc that decodes
// a JSON-encoded concat response from the HTTP response body. If the response
// has a non-200 status code, we will interpret that as an error and attempt to
// decode the specific error message from the response body. Primarily useful in
// a client.
func decodeHTTPGetAllToDoResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.GetAllToDoResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

// encodeHTTPGenericRequest is a transport/http.EncodeRequestFunc that
// JSON-encodes any request to the request body. Primarily useful in a client.
func encodeHTTPGenericRequest(_ context.Context, r *http.Request, request interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return err
	}
	r.Body = ioutil.NopCloser(&buf)
	return nil
}

// encodeHTTPGenericResponse is a transport/http.EncodeResponseFunc that encodes
// the response as JSON to the response writer. Primarily useful in a server.
func encodeHTTPGenericResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	if f, ok := response.(endpoint.Failer); ok && f.Failed() != nil {
		errorEncoder(ctx, f.Failed(), w)
		return nil
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return json.NewEncoder(w).Encode(response)
}
