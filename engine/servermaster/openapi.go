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

// OpenAPI provides API for servermaster.
type OpenAPI struct {
	jobMgr  JobManager
	execMgr ExecutorManager
}

// NewOpenAPI creates a new OpenAPI.
func NewOpenAPI(jobMgr JobManager, execMgr ExecutorManager) *OpenAPI {
	return &OpenAPI{
		jobMgr:  jobMgr,
		execMgr: execMgr,
	}
}

// RegisterOpenAPIRoutes registers routes for OpenAPI.
func RegisterOpenAPIRoutes(router *gin.Engine, openapi *OpenAPI) {
	v1 := router.Group("/api/v1")

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
			openapi.ForwardJobMaster(c)
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
	// TODO: Implement it.
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

// ForwardJobMaster forwards the request to job master.
func (o *OpenAPI) ForwardJobMaster(c *gin.Context) {
	tenantID := c.Query(apiOpVarTenantID)
	projectID := c.Query(apiOpVarProjectID)
	_, _ = tenantID, projectID
	// TODO: verify the talent and project info.

	jobID := c.Param(apiOpVarJobID)
	if jobID == "" {
		_ = c.AbortWithError(http.StatusBadRequest, errors.New("job id must not be empty"))
	}

	ctx := c.Request.Context()
	resp := o.jobMgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: jobID})
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
	addr, ok := o.execMgr.GetAddr(executorID)
	if !ok {
		_ = c.AbortWithError(http.StatusInternalServerError, errors.New("couldn't find executor address"))
		return
	}

	execURL := addr
	if !strings.HasPrefix(execURL, "http://") &&
		!strings.HasPrefix(execURL, "https://") {
		execURL = "http://" + execURL
	}
	u, err := url.Parse(execURL)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("invalid executor address: %s", addr))
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.ServeHTTP(c.Writer, c.Request)
}
