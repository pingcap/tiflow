// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/openapi"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/parser"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	parserpkg "github.com/pingcap/tiflow/dm/pkg/parser"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var (
	globalConfig = &Config{}
	// GlobalCtlClient is the globally used CtlClient in this package. Exposed to be used in test.
	GlobalCtlClient = &CtlClient{}

	re = regexp.MustCompile(`grpc: received message larger than max \((\d+) vs. (\d+)\)`)
)

// CtlClient used to get master client for dmctl.
type CtlClient struct {
	mu                  sync.RWMutex
	tls                 *toolutils.TLS
	conn                *grpc.ClientConn
	MasterGRPCClient    pb.MasterClient  // exposed to be used in test
	EtcdClient          *clientv3.Client // exposed to be used in export config
	MasterOpenapiClient *openapi.Client
}

func (c *CtlClient) updateMasterClient() error {
	var (
		err  error
		conn *grpc.ClientConn
	)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	endpoints := c.EtcdClient.Endpoints()
	for _, endpoint := range endpoints {
		//nolint:staticcheck
		conn, err = grpc.Dial(utils.UnwrapScheme(endpoint), c.tls.ToGRPCDialOption(), grpc.WithBackoffMaxDelay(3*time.Second), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
		if err != nil {
			continue
		}
		c.conn = conn
		c.MasterGRPCClient = pb.NewMasterClient(conn)

		if c.tls.TLSConfig() == nil {
			c.MasterOpenapiClient, err = openapi.NewClient("http://" + utils.UnwrapScheme(endpoint))
			if err != nil {
				continue
			}
		} else {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = c.tls.TLSConfig()
			httpCli := &http.Client{Transport: transport}
			c.MasterOpenapiClient, err = openapi.NewClient("https://"+utils.UnwrapScheme(endpoint), openapi.WithHTTPClient(httpCli))
			if err != nil {
				continue
			}
		}
		return nil
	}
	return terror.ErrCtlCreateMasterConn.AnnotateDelegate(err, "can't connect to %s", strings.Join(endpoints, ","))
}

func (c *CtlClient) sendRequest(
	ctx context.Context,
	reqName string,
	req interface{},
	respPointer interface{},
	opts ...interface{},
) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
	for _, o := range opts {
		params = append(params, reflect.ValueOf(o))
	}
	results := reflect.ValueOf(c.MasterGRPCClient).MethodByName(reqName).Call(params)

	reflect.ValueOf(respPointer).Elem().Set(results[0])
	errInterface := results[1].Interface()
	// nil can't pass type conversion, so we handle it separately
	if errInterface == nil {
		return nil
	}
	return errInterface.(error)
}

// SendRequest send request to master.
func SendRequest(ctx context.Context, reqName string, req interface{}, respPointer interface{}) error {
	err := GlobalCtlClient.sendRequest(ctx, reqName, req, respPointer)
	if err == nil {
		return nil
	}
	var opts []interface{}
	switch status.Code(err) {
	case codes.ResourceExhausted:
		matches := re.FindStringSubmatch(err.Error())
		if len(matches) == 3 {
			msgSize, err2 := strconv.Atoi(matches[1])
			if err2 == nil {
				log.L().Info("increase gRPC maximum message size", zap.Int("size", msgSize))
				opts = append(opts, grpc.MaxCallRecvMsgSize(msgSize))
			}
		}
	case codes.Unavailable:
	default:
		return err
	}

	failpoint.Inject("SkipUpdateMasterClient", func() {
		failpoint.Goto("bypass")
	})
	// update master client
	err = GlobalCtlClient.updateMasterClient()
	if err != nil {
		return err
	}
	failpoint.Label("bypass")

	// sendRequest again
	return GlobalCtlClient.sendRequest(ctx, reqName, req, respPointer, opts...)
}

// InitUtils inits necessary dmctl utils.
func InitUtils(cfg *Config) error {
	globalConfig = cfg
	return errors.Trace(InitClient(cfg.MasterAddr, cfg.Security))
}

// InitClient initializes dm-master client.
func InitClient(addr string, securityCfg config.Security) error {
	tls, err := toolutils.NewTLS(securityCfg.SSLCA, securityCfg.SSLCert, securityCfg.SSLKey, "", securityCfg.CertAllowedCN)
	if err != nil {
		return terror.ErrCtlInvalidTLSCfg.Delegate(err)
	}

	endpoints := strings.Split(addr, ",")
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
		TLS:                  tls.TLSConfig(),
	})
	if err != nil {
		return err
	}

	GlobalCtlClient = &CtlClient{
		tls:        tls,
		EtcdClient: etcdClient,
	}

	return GlobalCtlClient.updateMasterClient()
}

// GlobalConfig returns global dmctl config.
func GlobalConfig() *Config {
	return globalConfig
}

// PrintLinesf adds a wrap to support `\n` within `chzyer/readline`.
func PrintLinesf(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format, a...))
}

// PrettyPrintResponse prints a PRC response prettily.
func PrettyPrintResponse(resp proto.Message) {
	s, err := marshResponseToString(resp)
	if err != nil {
		PrintLinesf("%v", err)
	} else {
		fmt.Println(s)
	}
}

// PrettyPrintInterface prints an interface through encoding/json prettily.
func PrettyPrintInterface(resp interface{}) {
	s, err := json.MarshalIndent(resp, "", "    ")
	if err != nil {
		PrintLinesf("%v", err)
	} else {
		fmt.Println(string(s))
	}
}

func marshResponseToString(resp proto.Message) (string, error) {
	// encoding/json does not support proto Enum well
	mar := jsonpb.Marshaler{EmitDefaults: true, Indent: "    "}
	s, err := mar.MarshalToString(resp)
	return s, errors.Trace(err)
}

// PrettyPrintResponseWithCheckTask prints a RPC response may contain response Msg with check-task's response prettily.
// check-task's response may contain json-string when checking fail in `detail` field.
// ugly code, but it is a little hard to refine this because needing to convert type.
func PrettyPrintResponseWithCheckTask(resp proto.Message, subStr string) bool {
	var (
		err          error
		found        bool
		replacedStr  string
		marshaledStr string
		placeholder  = "PLACEHOLDER"
	)
	switch chr := resp.(type) {
	case *pb.StartTaskResponse:
		if strings.Contains(chr.CheckResult, subStr) {
			found = true
			rawMsg := chr.CheckResult
			chr.CheckResult = placeholder // replace Msg with placeholder
			marshaledStr, err = marshResponseToString(chr)
			if err == nil {
				replacedStr = strings.Replace(marshaledStr, placeholder, rawMsg, 1)
			}
		}
	case *pb.UpdateTaskResponse:
		if strings.Contains(chr.CheckResult, subStr) {
			found = true
			rawMsg := chr.CheckResult
			chr.CheckResult = placeholder // replace Msg with placeholder
			marshaledStr, err = marshResponseToString(chr)
			if err == nil {
				replacedStr = strings.Replace(marshaledStr, placeholder, rawMsg, 1)
			}
		}
	case *pb.CheckTaskResponse:
		if strings.Contains(chr.Msg, subStr) {
			found = true
			rawMsg := chr.Msg
			chr.Msg = placeholder // replace Msg with placeholder
			marshaledStr, err = marshResponseToString(chr)
			if err == nil {
				replacedStr = strings.Replace(marshaledStr, placeholder, rawMsg, 1)
			}
		}

	default:
		return false
	}

	if !found {
		return found
	}

	if err != nil {
		PrintLinesf("%v", err)
	} else {
		// add indent to make it prettily.
		replacedStr = strings.Replace(replacedStr, "detail: {", "   \tdetail: {", 1)
		fmt.Println(replacedStr)
	}
	return found
}

// GetFileContent reads and returns file's content.
func GetFileContent(fpath string) ([]byte, error) {
	content, err := os.ReadFile(fpath)
	if err != nil {
		return nil, errors.Annotate(err, "error in get file content")
	}
	return content, nil
}

// GetSourceArgs extracts sources from cmd.
func GetSourceArgs(cmd *cobra.Command) ([]string, error) {
	ret, err := cmd.Flags().GetStringSlice("source")
	if err != nil {
		PrintLinesf("error in parse `-s` / `--source`")
	}
	return ret, err
}

// ExtractSQLsFromArgs extract multiple sql from args.
func ExtractSQLsFromArgs(args []string) ([]string, error) {
	if len(args) == 0 {
		return nil, errors.New("args is empty")
	}

	concat := strings.TrimSpace(strings.Join(args, " "))
	concat = utils.TrimQuoteMark(concat)

	parser := parser.New()
	nodes, err := parserpkg.Parse(parser, concat, "", "")
	if err != nil {
		return nil, errors.Annotatef(err, "invalid sql '%s'", concat)
	}
	realSQLs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		realSQLs = append(realSQLs, node.Text())
	}
	if len(realSQLs) == 0 {
		return nil, errors.New("no valid SQLs")
	}

	return realSQLs, nil
}

// GetTaskNameFromArgOrFile tries to retrieve name from the file if arg is yaml-filename-like, otherwise returns arg directly.
func GetTaskNameFromArgOrFile(arg string) string {
	if !(strings.HasSuffix(arg, ".yaml") || strings.HasSuffix(arg, ".yml")) {
		return arg
	}
	var (
		content []byte
		err     error
	)
	if content, err = GetFileContent(arg); err != nil {
		return arg
	}
	cfg := config.NewTaskConfig()
	if err := cfg.Decode(string(content)); err != nil {
		return arg
	}
	return cfg.Name
}

// PrintCmdUsage prints the usage of the command.
func PrintCmdUsage(cmd *cobra.Command) {
	if err := cmd.Usage(); err != nil {
		fmt.Println("can't output command's usage:", err)
	}
}

// SendOpenapiRequest send openapi request to master.
func (c *CtlClient) sendOpenapiRequest(
	ctx context.Context,
	reqName string,
	params []interface{},
	reqEditors ...openapi.RequestEditorFn,
) (*http.Response, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	methodType, hasMethod := reflect.TypeOf(c.MasterOpenapiClient).MethodByName(reqName)
	if !hasMethod {
		return nil, errors.New("not found Openapi Client method " + reqName)
	}
	reflectParams := []reflect.Value{reflect.ValueOf(ctx)}
	for i, p := range params {
		// convert type
		pType := methodType.Type.In(i + 2)
		reflectParams = append(reflectParams, reflect.ValueOf(p).Convert(pType))
	}
	for _, e := range reqEditors {
		reflectParams = append(reflectParams, reflect.ValueOf(e))
	}

	results := reflect.ValueOf(c.MasterOpenapiClient).MethodByName(reqName).Call(reflectParams)
	errInterface := results[1].Interface()
	// nil can't pass type conversion, so we handle it separately
	if errInterface == nil {
		res := &http.Response{}
		reflect.ValueOf(&res).Elem().Set(results[0])
		return res, nil
	}
	return nil, errInterface.(error)
}

type SendToOpenapiError struct {
	error
	StructMsg *openapi.ErrorWithMessage
	StringMsg string
}

func (e SendToOpenapiError) Error() string {
	ptRes := prettyOpenapiRes{Result: false}
	if e.StructMsg != nil {
		ptRes.Message = e.StructMsg
	} else {
		ptRes.Message = e.StringMsg
	}
	s, err := json.MarshalIndent(ptRes, "", "    ")
	if err != nil {
		return err.Error()
	}
	return string(s)
}

// GetOriginMsg returns origin error msg to make print exception simple
func (e SendToOpenapiError) GetOriginMsg() interface{} {
	if e.StructMsg != nil {
		return e.StructMsg
	}
	return e.StringMsg
}

func SendOpenapiRequest(ctx context.Context, reqName string, params []interface{}, successCode int, respPointer interface{}) *SendToOpenapiError {
	httpRes, err := GlobalCtlClient.sendOpenapiRequest(ctx, reqName, params)
	if err != nil {
		return &SendToOpenapiError{StringMsg: err.Error()}
	}
	// handle httpRes
	switch httpRes.StatusCode {
	case successCode:
		if respPointer == nil {
			return nil
		}
		err = json.NewDecoder(httpRes.Body).Decode(&respPointer)
		if err != nil {
			return &SendToOpenapiError{StringMsg: err.Error()}
		}
	case http.StatusBadRequest:
		errResp := &openapi.ErrorWithMessage{}
		body, err := ioutil.ReadAll(httpRes.Body)
		if err != nil {
			return &SendToOpenapiError{StringMsg: err.Error()}
		}
		// try to make ErrorWithMessage
		err = json.Unmarshal(body, &errResp)
		if err != nil || errResp.ErrorMsg == "" {
			return &SendToOpenapiError{StringMsg: string(body)}
		}
		return &SendToOpenapiError{StructMsg: errResp}
	default:
		buffer := new(bytes.Buffer)
		err := httpRes.Write(buffer)
		if err != nil {
			return &SendToOpenapiError{StringMsg: err.Error()}
		}
		return &SendToOpenapiError{StringMsg: buffer.String()}
	}
	return nil
}

type prettyOpenapiRes struct {
	Result  bool
	Message interface{}
}

func PrettyPrintOpenapiResp(isSuccess bool, resp interface{}) {
	PrettyPrintInterface(prettyOpenapiRes{Result: isSuccess, Message: resp})
}
