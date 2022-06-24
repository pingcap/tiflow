// Copyright 2022 PingCAP, Inc.
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

package servermaster

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
)

const (
	// apiOpVarTenant is the key of tenant id in HTTP API.
	apiOpVarTenantID = "tenant_id"
	// apiOpVarProjectID is the key of project id in HTTP API.
	apiOpVarProjectID = "project_id"
	// apiOpVarJobID is the key of job id in HTTP API.
	apiOpVarJobID = "job_id"
)

// ServerInfoProvider provides server info.
type ServerInfoProvider interface {
	// IsLeader returns whether the server is leader.
	IsLeader() bool
	// LeaderAddr returns the address of leader.
	LeaderAddr() (string, bool)
	// JobManager returns the job manager instance.
	// It returns nil if the server is not leader.
	JobManager() (JobManager, bool)
	// ExecutorManager returns the executor manager instance.
	// It returns nil if the server is not leader.
	ExecutorManager() (ExecutorManager, bool)
}

// OpenAPI provides API for servermaster.
type OpenAPI struct {
	infoProvider ServerInfoProvider
}

// NewOpenAPI creates a new OpenAPI.
func NewOpenAPI(infoProvider ServerInfoProvider) *OpenAPI {
	return &OpenAPI{infoProvider: infoProvider}
}

// RegisterOpenAPIRoutes registers routes for OpenAPI.
func RegisterOpenAPIRoutes(router *gin.Engine, openapi *OpenAPI) {
	v1 := router.Group("/api/v1")
	v1.Use(openapi.ForwardToLeader)

	jobGroup := v1.Group("/jobs")
	jobGroup.GET("", openapi.ListJobs)
	jobGroup.POST("", openapi.SubmitJob)
	jobGroup.GET("/:job_id", openapi.QueryJob)
	// Gin doesn't support wildcard path including any explicit path.
	// So we need to route pause handler and cancel handler manually.
	// See https://github.com/gin-gonic/gin/issues/2016.
	jobGroup.Any("/:job_id/*action", func(c *gin.Context) {
		action := c.Param("action")
		switch action {
		case "/pause":
			if c.Request.Method != http.MethodPost {
				c.AbortWithStatus(http.StatusMethodNotAllowed)
			} else {
				openapi.PauseJob(c)
			}
		case "/cancel":
			if c.Request.Method != http.MethodPost {
				c.AbortWithStatus(http.StatusMethodNotAllowed)
			} else {
				openapi.CancelJob(c)
			}
		default:
			openapi.ForwardToJobMaster(c)
		}
	})
}

// ListJobs lists all jobs in servermaster.
// @Summary List jobs
// @Description lists all jobs in servermaster
// @Tags jobs
// @Accept json
// @Produce json
// @Param tenant query string false "tenant id"
// @Param project query string false "project id"
// @Success 200
// @Failure 400,500
// @Router /api/v1/jobs [get]
func (o *OpenAPI) ListJobs(c *gin.Context) {
	tenantID := c.Query(apiOpVarTenantID)
	projectID := c.Query(apiOpVarProjectID)
	_, _ = tenantID, projectID
	// TODO: Implement it.
	c.AbortWithStatus(http.StatusNotImplemented)
}

// SubmitJob submits a new job.
// @Summary Submit a job
// @Description submits a new job
// @Tags jobs
// @Accept json
// @Produce json
// @Success 202
// @Failure 400,500
// @Router	/api/v1/jobs [post]
func (o *OpenAPI) SubmitJob(c *gin.Context) {
	tenantID := c.Query(apiOpVarTenantID)
	projectID := c.Query(apiOpVarProjectID)
	jobID := c.Param(apiOpVarJobID)
	_, _, _ = tenantID, projectID, jobID
	// TODO: Implement it.
	c.AbortWithStatus(http.StatusNotImplemented)
}

// QueryJob queries detail information of a job.
// @Summary Query a job
// @Description query detail information of a job
// @Tags jobs
// @Accept json
// @Produce json
// @Param job_id  path  string  true  "job id"
// @Success 200
// @Failure 400,500
// @Router /api/v1/jobs/{job_id} [get]
func (o *OpenAPI) QueryJob(c *gin.Context) {
	tenantID := c.Query(apiOpVarTenantID)
	projectID := c.Query(apiOpVarProjectID)
	jobID := c.Param(apiOpVarJobID)
	_, _, _ = tenantID, projectID, jobID
	// TODO: Implement it.
	c.AbortWithStatus(http.StatusNotImplemented)
}

// PauseJob pauses a job.
// @Summary Pause a job
// @Description pause a job
// @Tags jobs
// @Accept json
// @Produce json
// @Param job_id  path  string  true  "job id"
// @Success 202
// @Failure 400,500
// @Router /api/v1/jobs/{job_id}/pause [post]
func (o *OpenAPI) PauseJob(c *gin.Context) {
	tenantID := c.Query(apiOpVarTenantID)
	projectID := c.Query(apiOpVarProjectID)
	jobID := c.Param(apiOpVarJobID)
	_, _, _ = tenantID, projectID, jobID
	// TODO: PauseJob will be removed in the future.
	c.AbortWithStatus(http.StatusNotImplemented)
}

// CancelJob cancels a job.
// @Summary Cancel a job
// @Description cancel a job
// @Tags jobs
// @Accept json
// @Produce json
// @Param job_id  path  string  true  "job id"
// @Success 202
// @Failure 400,500
// @Router /api/v1/jobs/{job_id}/cancel [post]
func (o *OpenAPI) CancelJob(c *gin.Context) {
	tenantID := c.Query(apiOpVarTenantID)
	projectID := c.Query(apiOpVarProjectID)
	jobID := c.Param(apiOpVarJobID)
	_, _, _ = tenantID, projectID, jobID
	// TODO: Implement it.
	c.AbortWithStatus(http.StatusNotImplemented)
}

// ForwardToJobMaster forwards the request to job master.
func (o *OpenAPI) ForwardToJobMaster(c *gin.Context) {
	tenantID := c.Query(apiOpVarTenantID)
	projectID := c.Query(apiOpVarProjectID)
	_, _ = tenantID, projectID
	// TODO: verify the talent and project info.

	jobID := c.Param(apiOpVarJobID)
	if jobID == "" {
		_ = c.AbortWithError(http.StatusBadRequest, errors.New("job id must not be empty"))
		return
	}

	jobMgr, ok := o.infoProvider.JobManager()
	if !ok {
		_ = c.AbortWithError(http.StatusServiceUnavailable, errors.New("job manager is not initialized"))
		return
	}
	executorMgr, ok := o.infoProvider.ExecutorManager()
	if !ok {
		_ = c.AbortWithError(http.StatusServiceUnavailable, errors.New("executor manager is not initialized"))
		return
	}

	ctx := c.Request.Context()
	resp := jobMgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: jobID})
	if resp.Err != nil {
		if resp.Err.Code == pb.ErrorCode_UnKnownJob {
			c.AbortWithStatus(http.StatusNotFound)
		} else {
			_ = c.AbortWithError(http.StatusInternalServerError, errors.New(resp.Err.String()))
		}
		return
	}

	if resp.Status != pb.QueryJobResponse_online {
		_ = c.AbortWithError(http.StatusServiceUnavailable, errors.New("job is not online"))
		return
	}
	if resp.JobMasterInfo == nil {
		_ = c.AbortWithError(http.StatusInternalServerError, errors.New("couldn't find job master info"))
		return
	}

	executorID := model.ExecutorID(resp.JobMasterInfo.ExecutorId)
	addr, ok := executorMgr.GetAddr(executorID)
	if !ok {
		_ = c.AbortWithError(http.StatusInternalServerError, errors.New("couldn't find executor address"))
		return
	}

	u, err := o.parseURL(addr)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("invalid executor address: %s", addr))
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.ServeHTTP(c.Writer, c.Request)
	c.Abort()
}

// ForwardToLeader forwards the request to leader if current server is not a leader.
func (o *OpenAPI) ForwardToLeader(c *gin.Context) {
	if !o.infoProvider.IsLeader() {
		leaderAddr, ok := o.infoProvider.LeaderAddr()
		if !ok {
			_ = c.AbortWithError(http.StatusServiceUnavailable, errors.New("leader is not ready"))
			return
		}
		u, err := o.parseURL(leaderAddr)
		if err != nil {
			if err != nil {
				_ = c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("invalid executor address: %s", leaderAddr))
				return
			}
		}
		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.ServeHTTP(c.Writer, c.Request)
		c.Abort()
	} else {
		c.Next()
	}
}

func (o *OpenAPI) parseURL(addrOrURL string) (*url.URL, error) {
	rawURL := addrOrURL
	if !strings.HasPrefix(rawURL, "http://") &&
		!strings.HasPrefix(rawURL, "https://") {
		// TODO: Use https if tls config is provided.
		rawURL = "http://" + rawURL
	}
	return url.Parse(rawURL)
}
