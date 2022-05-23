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
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"

	"github.com/gin-gonic/gin"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pb"
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
	server *Server
}

// NewOpenAPI creates a new OpenAPI.
func NewOpenAPI(server *Server) *OpenAPI {
	return &OpenAPI{server: server}
}

// RegisterOpenAPIRoutes registers routes for OpenAPI.
func RegisterOpenAPIRoutes(router *gin.Engine, api *OpenAPI) {
	v1 := router.Group("/api/v1")

	// job API
	jobGroup := v1.Group("/jobs")
	jobGroup.GET("", api.ListJobs)
	jobGroup.POST("", api.SubmitJob)
	jobGroup.GET("/:job_id", api.QueryJob)
	jobGroup.Any("/:job_id/*action", func(c *gin.Context) {
		action := c.Param("action")
		switch action {
		case "/pause":
			if c.Request.Method != http.MethodPost {
				c.AbortWithStatus(http.StatusMethodNotAllowed)
			} else {
				api.PauseJob(c)
			}
		case "/cancel":
			if c.Request.Method != http.MethodPost {
				c.AbortWithStatus(http.StatusMethodNotAllowed)
			} else {
				api.CancelJob(c)
			}
		default:
			api.ForwardJobMaster(c)
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
	c.Status(http.StatusNotImplemented)
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
	c.Status(http.StatusNotImplemented)
}

// QueryJob queries a detail information of a job.
// @Summary Query a job
// @Description query a detail information of a job
// @Tags jobs
// @Accept json
// @Produce json
// @Param job_id  path  string  true  "job id"
// @Success 200
// @Failure 400,500
// @Router /api/v1/jobs/{job_id} [get]
func (o *OpenAPI) QueryJob(c *gin.Context) {
	jobID := c.Param(apiOpVarJobID)
	_ = jobID
	c.Status(http.StatusNotImplemented)
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
	jobID := c.Param(apiOpVarJobID)
	_ = jobID
	c.Status(http.StatusNotImplemented)
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
	jobID := c.Param(apiOpVarJobID)
	_ = jobID
	c.Status(http.StatusNotImplemented)
}

// ForwardJobMaster forwards api requests to job master.
func (o *OpenAPI) ForwardJobMaster(c *gin.Context) {
	jobID := c.Param(apiOpVarJobID)
	tenantID := c.Query(apiOpVarTenantID)
	projectID := c.Query(apiOpVarProjectID)
	_, _, _ = jobID, tenantID, projectID

	ctx := c.Request.Context()
	resp, err := o.server.QueryJob(ctx, &pb.QueryJobRequest{JobId: jobID})
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	if resp.Err != nil {
		if resp.Err.Code == pb.ErrorCode_UnKnownJob {
			c.AbortWithStatus(http.StatusNotFound)
		} else {
			c.AbortWithError(http.StatusInternalServerError, errors.New(resp.Err.String()))
		}
		return
	}
	if resp.JobMasterInfo == nil {
		c.AbortWithError(http.StatusInternalServerError, errors.New("couldn't find job master info"))
		return
	}
	// TODO: check tenant id and project id.
	executorID := model.ExecutorID(resp.JobMasterInfo.ExecutorId)
	addr, ok := o.server.executorManager.GetAddr(executorID)
	if !ok {
		c.AbortWithError(http.StatusInternalServerError, errors.New("executor not found"))
		return
	}

	addrPort, err := netip.ParseAddrPort(addr)
	if err == nil {
		addr = "http://" + addrPort.String()
	}
	u, err := url.Parse(addr)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.ServeHTTP(c.Writer, c.Request)
}
