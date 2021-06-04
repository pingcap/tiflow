package cdc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

const (
	// APIOpVarChangefeeds is the key of list option in HTTP API
	APIOpVarChangefeeds = "state"
)

// JSONTime used to wrap time into json format
type JSONTime time.Time

// MarshalJSON use to specify the time format
func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02 15:04:05.000"))
	return []byte(stamp), nil
}

// err of cdc http api
type httpError struct {
	Message string              `json:"error"`
	Code    errors.RFCErrorCode `json:"errorCode"`
}

// ChangefeedCommonInfo holds some common usage information of a changefeed and use by RESTful API only.
type ChangefeedCommonInfo struct {
	ID             string              `json:"id"`
	FeedState      string              `json:"state"`
	CheckpointTSO  uint64              `json:"checkpoint-tso"`
	CheckpointTime JSONTime            `json:"checkpoint-time"`
	RunningError   *model.RunningError `json:"error"`
}

// handleHealth check if is this server is health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// handleChangefeeds dispatch the request to the specified handleFunc according to the request method.
func (s *Server) handleChangefeeds(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		s.handleChangefeedsList(w, req)
		return
	}
}

// handleChangefeedsList will only received request with Get method from dispatcher.
func (s *Server) handleChangefeedsList(w http.ResponseWriter, req *http.Request) {
	s.ownerLock.RLock()
	defer s.ownerLock.RUnlock()
	if s.owner == nil {
		writeErrorJSON(w, http.StatusBadRequest, *cerror.ErrNotOwner)
		return
	}

	err := req.ParseForm()
	if err != nil {
		writeInternalServerErrorJSON(w, cerror.WrapError(cerror.ErrInternalServerError, err))
		return
	}

	statuses, err := s.owner.etcdClient.GetAllChangeFeedStatus(req.Context())
	changefeedIDs := make(map[string]struct{}, len(statuses))
	if err != nil {
		writeInternalServerErrorJSON(w, err)
		return
	}
	for cid := range statuses {
		changefeedIDs[cid] = struct{}{}
	}

	state := req.Form.Get(APIOpVarChangefeeds)

	resps := make([]*ChangefeedCommonInfo, 0)
	for changefeedID := range changefeedIDs {
		cf, status, feedState, err := s.owner.collectChangefeedInfo(req.Context(), changefeedID)
		if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
			writeInternalServerErrorJSON(w, err)
			return
		}
		if !httputil.IsFilter(state, feedState) {
			continue
		}
		feedInfo, err := s.owner.etcdClient.GetChangeFeedInfo(req.Context(), changefeedID)
		if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
			writeInternalServerErrorJSON(w, err)
			return
		}
		resp := &ChangefeedCommonInfo{
			ID:        changefeedID,
			FeedState: string(feedState),
		}

		if cf != nil {
			// httpErr := httpError{cf.info.Error.Message, errors.RFCErrorCode(cf.info.Error.Code)}
			resp.RunningError = cf.info.Error
		} else if feedInfo != nil {
			// httpErr := httpError{feedInfo.Error.Message, errors.RFCErrorCode(feedInfo.Error.Code)}
			resp.RunningError = feedInfo.Error
		}

		if status != nil {
			resp.CheckpointTSO = status.CheckpointTs
			tm := oracle.GetTimeFromTS(status.CheckpointTs)
			resp.CheckpointTime = JSONTime(tm)
		}
		resps = append(resps, resp)
	}
	writeData(w, resps)
}

func writeInternalServerErrorJSON(w http.ResponseWriter, err error) {
	writeErrorJSON(w, http.StatusInternalServerError, *cerror.ErrInternalServerError.Wrap(err))
}

func writeErrorJSON(w http.ResponseWriter, statusCode int, cerr errors.Error) {
	httpErr := httpError{Code: cerr.RFCCode(), Message: cerr.GetMsg()}
	jsonStr, err := json.MarshalIndent(httpErr, "", " ")
	if err != nil {
		log.Error("invalid json data", zap.Reflect("data", err), zap.Error(err))
		return
	}
	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(jsonStr)
	if err != nil {
		log.Error("fail to write data", zap.Error(err))
	}
}
