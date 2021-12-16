package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	terrors "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/retry"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	defaultBackoffBaseDelayInMs = 500
	defaultBackoffMaxDelayInMs  = 30 * 1000
	defaultMaxRetries           = 10
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Request allows for building up a request to cdc server in a chained fasion.
// Any errors are stored until the end of your call, so you only have to
// check once.
type Request struct {
	c *RESTClient
	// TODO: implement client-side throttling
	rateLimiter *rate.Limiter
	timeout     time.Duration

	// generic components accessible via method setters
	verb       string
	pathPrefix string
	params     url.Values
	headers    http.Header

	// retry options
	backoffBaseDelay time.Duration // milliseconds
	backoffMaxDelay  time.Duration // milliseconds
	maxRetries       int64

	//output
	err  error
	body io.Reader
}

// NewRequest creates a new request
func NewRequest(c *RESTClient) *Request {
	var pathPrefix string
	if c.base != nil {
		pathPrefix = path.Join("/", c.base.Path, c.versionedAPIPath)
	} else {
		pathPrefix = path.Join("/", c.versionedAPIPath)
	}

	var timeout time.Duration
	if c.Client != nil {
		timeout = c.Client.Timeout
	}

	r := &Request{
		c:           c,
		rateLimiter: c.rateLimiter,
		timeout:     timeout,
		pathPrefix:  pathPrefix,
	}
	r.SetHeader("Accept", "application/json")
	return r
}

// NewRequestWithClient creates a Request with an embedded RESTClient for test
func NewRequestWithClient(base *url.URL, versionedAPIPath string, client *httputil.Client) *Request {
	return NewRequest(&RESTClient{
		base:             base,
		versionedAPIPath: versionedAPIPath,
		Client:           client,
	})
}

// Prefix adds segments to the begining of request url
func (r *Request) Prefix(segments ...string) *Request {
	if r.err != nil {
		return r
	}

	r.pathPrefix = path.Join(r.pathPrefix, path.Join(segments...))
	return r
}

// URI sets the
func (r *Request) URI(uri string) *Request {
	if r.err != nil {
		return r
	}
	u, err := url.Parse(uri)
	if err != nil {
		r.err = err
		return r
	}
	r.pathPrefix = u.Path
	vals := u.Query()
	if len(vals) > 0 {
		if r.params == nil {
			r.params = make(url.Values)
		}
		for k, v := range vals {
			r.params[k] = v
		}
	}
	return r
}

// Param sets the http request query params
func (r *Request) Param(name, value string) *Request {
	if r.err != nil {
		return r
	}
	if r.params == nil {
		r.params = make(url.Values)
	}
	r.params[name] = append(r.params[name], value)
	return r
}

// Verb sets the verb this request will use.
func (r *Request) Verb(verb string) *Request {
	r.verb = verb
	return r
}

// SetHeader set the http request header
func (r *Request) SetHeader(key string, values ...string) *Request {
	if r.headers == nil {
		r.headers = http.Header{}
	}
	r.headers.Del(key)
	for _, value := range values {
		r.headers.Add(key, value)
	}
	return r
}

// Timeout specifies overall timeout of a request.
func (r *Request) Timeout(d time.Duration) *Request {
	if r.err != nil {
		return r
	}
	r.timeout = d
	return r
}

// BackoffBaseDelay specifies the base backoff sleep duration
func (r *Request) BackoffBaseDelay(delay time.Duration) *Request {
	if r.err != nil {
		return r
	}
	r.backoffBaseDelay = delay
	return r
}

// BackoffMaxDelay specifies the maximum backoff sleep duration
func (r *Request) BackoffMaxDelay(delay time.Duration) *Request {
	if r.err != nil {
		return r
	}
	r.backoffMaxDelay = delay
	return r
}

// MaxRetries specifies the maximum times a request will retry
func (r *Request) MaxRetries(maxRetries int64) *Request {
	if r.err != nil {
		return r
	}
	if maxRetries > 0 {
		r.maxRetries = maxRetries
	} else {
		r.maxRetries = defaultMaxRetries
	}
	return r
}

// Body makes http request use obj as its body.
func (r *Request) Body(obj interface{}) *Request {
	if r.err != nil {
		return r
	}

	// only supports two types now:
	//   1. io.Reader
	//   2. type which can be json marshalled
	if rd, ok := obj.(io.Reader); ok {
		r.body = rd
	} else {
		b, err := json.Marshal(obj)
		if err != nil {
			r.err = err
			return r
		}
		r.body = bytes.NewReader(b)
		r.SetHeader("Content-Type", "application/json")
	}
	return r
}

// URL returns the current working URL.
func (r *Request) URL() *url.URL {
	p := r.pathPrefix

	finalURL := &url.URL{}
	if r.c.base != nil {
		*finalURL = *r.c.base
	}
	finalURL.Path = p

	query := url.Values{}
	for key, values := range r.params {
		for _, value := range values {
			query.Add(key, value)
		}
	}

	finalURL.RawQuery = query.Encode()
	return finalURL
}

func (r *Request) newHttpRequest(ctx context.Context) (*http.Request, error) {
	url := r.URL().String()
	req, err := http.NewRequest(r.verb, url, r.body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	req.Header = r.headers
	return req, nil
}

// Do formats and executes the request.
func (r *Request) Do(ctx context.Context) (res *Result) {
	if r.err != nil {
		log.Info("error in request", zap.Error(r.err))
		return &Result{err: r.err}
	}

	client := r.c.Client
	if client == nil {
		client = &httputil.Client{}
	}

	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	baseDelay := r.backoffBaseDelay.Milliseconds()
	if baseDelay == 0 {
		baseDelay = defaultBackoffBaseDelayInMs
	}
	maxDelay := r.backoffMaxDelay.Milliseconds()
	if maxDelay == 0 {
		maxDelay = defaultBackoffMaxDelayInMs
	}
	maxRetries := r.maxRetries
	if maxRetries == 0 {
		maxRetries = defaultMaxRetries
	}

	err := retry.Do(ctx, func() error {
		req, err := r.newHttpRequest(ctx)
		if err != nil {
			return err
		}
		// rewind the request body when req.body is not nil
		if seeker, ok := r.body.(io.Seeker); ok && r.body != nil {
			if _, err := seeker.Seek(0, 0); err != nil {
				return terrors.Annotatef(err, "failed to seek to the beginning of request body:%v", r)
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Error("failed to send a http request", zap.Error(err))
			return err
		}

		defer func() {
			if resp == nil {
				return
			}
			// close the body to let the TCP connection be reused after reconnecting
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()

		res = r.checkResponse(resp)
		if res.Error() != nil {
			return res.Error()
		}
		return nil
	},
		retry.WithBackoffBaseDelay(baseDelay),
		retry.WithBackoffMaxDelay(maxDelay),
		retry.WithMaxTries(maxRetries),
		retry.WithIsRetryableErr(cerrors.IsRetryableError),
	)

	if res == nil && err != nil {
		return &Result{err: err}
	}

	return
}

// check http response and unmarshal error message if necessary
func (r *Request) checkResponse(resp *http.Response) *Result {
	var body []byte
	if resp.Body != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return &Result{err: err}
		}
		body = data
	}

	contentType := resp.Header.Get("Content-Type")
	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusPartialContent {
		var jsonErr model.HTTPError
		err := json.Unmarshal(body, &jsonErr)
		if err == nil {
			err = errors.New(jsonErr.Error)
		}

		return &Result{
			body:        body,
			contentType: contentType,
			statusCode:  resp.StatusCode,
			err:         err,
		}
	}

	return &Result{
		body:        body,
		contentType: contentType,
		statusCode:  resp.StatusCode,
	}
}

// Result contains the result of calling Request.Do().
type Result struct {
	body        []byte
	contentType string
	err         error
	statusCode  int
}

// Raw returns the raw result.
func (r Result) Raw() ([]byte, error) {
	return r.body, r.err
}

// Error returns the request error
func (r Result) Error() error {
	return r.err
}

// Into stores the http response body into obj
func (r Result) Into(obj interface{}) error {
	if r.err != nil {
		return r.err
	}

	if len(r.body) == 0 {
		return fmt.Errorf("0-length response with status code: %d", r.statusCode)
	}

	if err := json.Unmarshal(r.body, obj); err != nil {
		return err
	}

	return nil
}
