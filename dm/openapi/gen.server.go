// Package openapi provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen version v1.9.0 DO NOT EDIT.
package openapi

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/deepmap/oapi-codegen/pkg/runtime"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
)

// ServerInterface represents all server handlers.
type ServerInterface interface {
	// get cluster master node list
	// (GET /api/v1/cluster/masters)
	DMAPIGetClusterMasterList(c *gin.Context)
	// offline master node
	// (DELETE /api/v1/cluster/masters/{master-name})
	DMAPIOfflineMasterNode(c *gin.Context, masterName string)
	// get cluster worker node list
	// (GET /api/v1/cluster/workers)
	DMAPIGetClusterWorkerList(c *gin.Context)
	// offline worker node
	// (DELETE /api/v1/cluster/workers/{worker-name})
	DMAPIOfflineWorkerNode(c *gin.Context, workerName string)
	// get doc json
	// (GET /api/v1/dm.json)
	GetDocJSON(c *gin.Context)
	// get doc html
	// (GET /api/v1/docs)
	GetDocHTML(c *gin.Context)
	// get data source list
	// (GET /api/v1/sources)
	DMAPIGetSourceList(c *gin.Context, params DMAPIGetSourceListParams)
	// create new data source
	// (POST /api/v1/sources)
	DMAPICreateSource(c *gin.Context)
	// delete a data source
	// (DELETE /api/v1/sources/{source-name})
	DMAPIDeleteSource(c *gin.Context, sourceName string, params DMAPIDeleteSourceParams)
	// pause relay log function for the data source
	// (POST /api/v1/sources/{source-name}/pause-relay)
	DMAPIPauseRelay(c *gin.Context, sourceName string)
	// resume relay log function for the data source
	// (POST /api/v1/sources/{source-name}/resume-relay)
	DMAPIResumeRelay(c *gin.Context, sourceName string)
	// get source schema list
	// (GET /api/v1/sources/{source-name}/schemas)
	DMAPIGetSourceSchemaList(c *gin.Context, sourceName string)
	// get source table list
	// (GET /api/v1/sources/{source-name}/schemas/{schema-name})
	DMAPIGetSourceTableList(c *gin.Context, sourceName string, schemaName string)
	// enable relay log function for the data source
	// (POST /api/v1/sources/{source-name}/start-relay)
	DMAPIStartRelay(c *gin.Context, sourceName string)
	// get the current status of the data source
	// (GET /api/v1/sources/{source-name}/status)
	DMAPIGetSourceStatus(c *gin.Context, sourceName string)
	// disable relay log function for the data source
	// (POST /api/v1/sources/{source-name}/stop-relay)
	DMAPIStopRelay(c *gin.Context, sourceName string)
	// transfer source to a free worker
	// (POST /api/v1/sources/{source-name}/transfer)
	DMAPITransferSource(c *gin.Context, sourceName string)
	// get task list
	// (GET /api/v1/tasks)
	DMAPIGetTaskList(c *gin.Context)
	// create and start task
	// (POST /api/v1/tasks)
	DMAPIStartTask(c *gin.Context)
	// delete and stop task
	// (DELETE /api/v1/tasks/{task-name})
	DMAPIDeleteTask(c *gin.Context, taskName string, params DMAPIDeleteTaskParams)
	// pause task
	// (POST /api/v1/tasks/{task-name}/pause)
	DMAPIPauseTask(c *gin.Context, taskName string)
	// resume task
	// (POST /api/v1/tasks/{task-name}/resume)
	DMAPIResumeTask(c *gin.Context, taskName string)
	// get task source schema list
	// (GET /api/v1/tasks/{task-name}/sources/{source-name}/schemas)
	DMAPIGetSchemaListByTaskAndSource(c *gin.Context, taskName string, sourceName string)
	// get task source table list
	// (GET /api/v1/tasks/{task-name}/sources/{source-name}/schemas/{schema-name})
	DMAPIGetTableListByTaskAndSource(c *gin.Context, taskName string, sourceName string, schemaName string)
	// delete task source table structure
	// (DELETE /api/v1/tasks/{task-name}/sources/{source-name}/schemas/{schema-name}/{table-name})
	DMAPIDeleteTableStructure(c *gin.Context, taskName string, sourceName string, schemaName string, tableName string)
	// get task source table structure
	// (GET /api/v1/tasks/{task-name}/sources/{source-name}/schemas/{schema-name}/{table-name})
	DMAPIGetTableStructure(c *gin.Context, taskName string, sourceName string, schemaName string, tableName string)
	// operate task source table structure
	// (PUT /api/v1/tasks/{task-name}/sources/{source-name}/schemas/{schema-name}/{table-name})
	DMAPIOperateTableStructure(c *gin.Context, taskName string, sourceName string, schemaName string, tableName string)
	// get task status
	// (GET /api/v1/tasks/{task-name}/status)
	DMAPIGetTaskStatus(c *gin.Context, taskName string, params DMAPIGetTaskStatusParams)
}

// ServerInterfaceWrapper converts contexts to parameters.
type ServerInterfaceWrapper struct {
	Handler            ServerInterface
	HandlerMiddlewares []MiddlewareFunc
}

type MiddlewareFunc func(c *gin.Context)

// DMAPIGetClusterMasterList operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetClusterMasterList(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetClusterMasterList(c)
}

// DMAPIOfflineMasterNode operation middleware
func (siw *ServerInterfaceWrapper) DMAPIOfflineMasterNode(c *gin.Context) {
	var err error

	// ------------- Path parameter "master-name" -------------
	var masterName string

	err = runtime.BindStyledParameter("simple", false, "master-name", c.Param("master-name"), &masterName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter master-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIOfflineMasterNode(c, masterName)
}

// DMAPIGetClusterWorkerList operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetClusterWorkerList(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetClusterWorkerList(c)
}

// DMAPIOfflineWorkerNode operation middleware
func (siw *ServerInterfaceWrapper) DMAPIOfflineWorkerNode(c *gin.Context) {
	var err error

	// ------------- Path parameter "worker-name" -------------
	var workerName string

	err = runtime.BindStyledParameter("simple", false, "worker-name", c.Param("worker-name"), &workerName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter worker-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIOfflineWorkerNode(c, workerName)
}

// GetDocJSON operation middleware
func (siw *ServerInterfaceWrapper) GetDocJSON(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.GetDocJSON(c)
}

// GetDocHTML operation middleware
func (siw *ServerInterfaceWrapper) GetDocHTML(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.GetDocHTML(c)
}

// DMAPIGetSourceList operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetSourceList(c *gin.Context) {
	var err error

	// Parameter object where we will unmarshal all parameters from the context
	var params DMAPIGetSourceListParams

	// ------------- Optional query parameter "with_status" -------------
	if paramValue := c.Query("with_status"); paramValue != "" {
	}

	err = runtime.BindQueryParameter("form", true, false, "with_status", c.Request.URL.Query(), &params.WithStatus)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter with_status: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetSourceList(c, params)
}

// DMAPICreateSource operation middleware
func (siw *ServerInterfaceWrapper) DMAPICreateSource(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPICreateSource(c)
}

// DMAPIDeleteSource operation middleware
func (siw *ServerInterfaceWrapper) DMAPIDeleteSource(c *gin.Context) {
	var err error

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	// Parameter object where we will unmarshal all parameters from the context
	var params DMAPIDeleteSourceParams

	// ------------- Optional query parameter "force" -------------
	if paramValue := c.Query("force"); paramValue != "" {
	}

	err = runtime.BindQueryParameter("form", true, false, "force", c.Request.URL.Query(), &params.Force)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter force: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIDeleteSource(c, sourceName, params)
}

// DMAPIPauseRelay operation middleware
func (siw *ServerInterfaceWrapper) DMAPIPauseRelay(c *gin.Context) {
	var err error

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIPauseRelay(c, sourceName)
}

// DMAPIResumeRelay operation middleware
func (siw *ServerInterfaceWrapper) DMAPIResumeRelay(c *gin.Context) {
	var err error

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIResumeRelay(c, sourceName)
}

// DMAPIGetSourceSchemaList operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetSourceSchemaList(c *gin.Context) {
	var err error

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetSourceSchemaList(c, sourceName)
}

// DMAPIGetSourceTableList operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetSourceTableList(c *gin.Context) {
	var err error

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	// ------------- Path parameter "schema-name" -------------
	var schemaName string

	err = runtime.BindStyledParameter("simple", false, "schema-name", c.Param("schema-name"), &schemaName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter schema-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetSourceTableList(c, sourceName, schemaName)
}

// DMAPIStartRelay operation middleware
func (siw *ServerInterfaceWrapper) DMAPIStartRelay(c *gin.Context) {
	var err error

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIStartRelay(c, sourceName)
}

// DMAPIGetSourceStatus operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetSourceStatus(c *gin.Context) {
	var err error

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetSourceStatus(c, sourceName)
}

// DMAPIStopRelay operation middleware
func (siw *ServerInterfaceWrapper) DMAPIStopRelay(c *gin.Context) {
	var err error

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIStopRelay(c, sourceName)
}

// DMAPITransferSource operation middleware
func (siw *ServerInterfaceWrapper) DMAPITransferSource(c *gin.Context) {
	var err error

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPITransferSource(c, sourceName)
}

// DMAPIGetTaskList operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetTaskList(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetTaskList(c)
}

// DMAPIStartTask operation middleware
func (siw *ServerInterfaceWrapper) DMAPIStartTask(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIStartTask(c)
}

// DMAPIDeleteTask operation middleware
func (siw *ServerInterfaceWrapper) DMAPIDeleteTask(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// Parameter object where we will unmarshal all parameters from the context
	var params DMAPIDeleteTaskParams

	// ------------- Optional query parameter "source_name_list" -------------
	if paramValue := c.Query("source_name_list"); paramValue != "" {
	}

	err = runtime.BindQueryParameter("form", true, false, "source_name_list", c.Request.URL.Query(), &params.SourceNameList)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source_name_list: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIDeleteTask(c, taskName, params)
}

// DMAPIPauseTask operation middleware
func (siw *ServerInterfaceWrapper) DMAPIPauseTask(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIPauseTask(c, taskName)
}

// DMAPIResumeTask operation middleware
func (siw *ServerInterfaceWrapper) DMAPIResumeTask(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIResumeTask(c, taskName)
}

// DMAPIGetSchemaListByTaskAndSource operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetSchemaListByTaskAndSource(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetSchemaListByTaskAndSource(c, taskName, sourceName)
}

// DMAPIGetTableListByTaskAndSource operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetTableListByTaskAndSource(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	// ------------- Path parameter "schema-name" -------------
	var schemaName string

	err = runtime.BindStyledParameter("simple", false, "schema-name", c.Param("schema-name"), &schemaName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter schema-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetTableListByTaskAndSource(c, taskName, sourceName, schemaName)
}

// DMAPIDeleteTableStructure operation middleware
func (siw *ServerInterfaceWrapper) DMAPIDeleteTableStructure(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	// ------------- Path parameter "schema-name" -------------
	var schemaName string

	err = runtime.BindStyledParameter("simple", false, "schema-name", c.Param("schema-name"), &schemaName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter schema-name: %s", err)})
		return
	}

	// ------------- Path parameter "table-name" -------------
	var tableName string

	err = runtime.BindStyledParameter("simple", false, "table-name", c.Param("table-name"), &tableName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter table-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIDeleteTableStructure(c, taskName, sourceName, schemaName, tableName)
}

// DMAPIGetTableStructure operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetTableStructure(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	// ------------- Path parameter "schema-name" -------------
	var schemaName string

	err = runtime.BindStyledParameter("simple", false, "schema-name", c.Param("schema-name"), &schemaName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter schema-name: %s", err)})
		return
	}

	// ------------- Path parameter "table-name" -------------
	var tableName string

	err = runtime.BindStyledParameter("simple", false, "table-name", c.Param("table-name"), &tableName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter table-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetTableStructure(c, taskName, sourceName, schemaName, tableName)
}

// DMAPIOperateTableStructure operation middleware
func (siw *ServerInterfaceWrapper) DMAPIOperateTableStructure(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// ------------- Path parameter "source-name" -------------
	var sourceName string

	err = runtime.BindStyledParameter("simple", false, "source-name", c.Param("source-name"), &sourceName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source-name: %s", err)})
		return
	}

	// ------------- Path parameter "schema-name" -------------
	var schemaName string

	err = runtime.BindStyledParameter("simple", false, "schema-name", c.Param("schema-name"), &schemaName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter schema-name: %s", err)})
		return
	}

	// ------------- Path parameter "table-name" -------------
	var tableName string

	err = runtime.BindStyledParameter("simple", false, "table-name", c.Param("table-name"), &tableName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter table-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIOperateTableStructure(c, taskName, sourceName, schemaName, tableName)
}

// DMAPIGetTaskStatus operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetTaskStatus(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// Parameter object where we will unmarshal all parameters from the context
	var params DMAPIGetTaskStatusParams

	// ------------- Optional query parameter "source_name_list" -------------
	if paramValue := c.Query("source_name_list"); paramValue != "" {
	}

	err = runtime.BindQueryParameter("form", true, false, "source_name_list", c.Request.URL.Query(), &params.SourceNameList)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter source_name_list: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetTaskStatus(c, taskName, params)
}

// GinServerOptions provides options for the Gin server.
type GinServerOptions struct {
	BaseURL     string
	Middlewares []MiddlewareFunc
}

// RegisterHandlers creates http.Handler with routing matching OpenAPI spec.
func RegisterHandlers(router *gin.Engine, si ServerInterface) *gin.Engine {
	return RegisterHandlersWithOptions(router, si, GinServerOptions{})
}

// RegisterHandlersWithOptions creates http.Handler with additional options
func RegisterHandlersWithOptions(router *gin.Engine, si ServerInterface, options GinServerOptions) *gin.Engine {
	wrapper := ServerInterfaceWrapper{
		Handler:            si,
		HandlerMiddlewares: options.Middlewares,
	}

	router.GET(options.BaseURL+"/api/v1/cluster/masters", wrapper.DMAPIGetClusterMasterList)

	router.DELETE(options.BaseURL+"/api/v1/cluster/masters/:master-name", wrapper.DMAPIOfflineMasterNode)

	router.GET(options.BaseURL+"/api/v1/cluster/workers", wrapper.DMAPIGetClusterWorkerList)

	router.DELETE(options.BaseURL+"/api/v1/cluster/workers/:worker-name", wrapper.DMAPIOfflineWorkerNode)

	router.GET(options.BaseURL+"/api/v1/dm.json", wrapper.GetDocJSON)

	router.GET(options.BaseURL+"/api/v1/docs", wrapper.GetDocHTML)

	router.GET(options.BaseURL+"/api/v1/sources", wrapper.DMAPIGetSourceList)

	router.POST(options.BaseURL+"/api/v1/sources", wrapper.DMAPICreateSource)

	router.DELETE(options.BaseURL+"/api/v1/sources/:source-name", wrapper.DMAPIDeleteSource)

	router.POST(options.BaseURL+"/api/v1/sources/:source-name/pause-relay", wrapper.DMAPIPauseRelay)

	router.POST(options.BaseURL+"/api/v1/sources/:source-name/resume-relay", wrapper.DMAPIResumeRelay)

	router.GET(options.BaseURL+"/api/v1/sources/:source-name/schemas", wrapper.DMAPIGetSourceSchemaList)

	router.GET(options.BaseURL+"/api/v1/sources/:source-name/schemas/:schema-name", wrapper.DMAPIGetSourceTableList)

	router.POST(options.BaseURL+"/api/v1/sources/:source-name/start-relay", wrapper.DMAPIStartRelay)

	router.GET(options.BaseURL+"/api/v1/sources/:source-name/status", wrapper.DMAPIGetSourceStatus)

	router.POST(options.BaseURL+"/api/v1/sources/:source-name/stop-relay", wrapper.DMAPIStopRelay)

	router.POST(options.BaseURL+"/api/v1/sources/:source-name/transfer", wrapper.DMAPITransferSource)

	router.GET(options.BaseURL+"/api/v1/tasks", wrapper.DMAPIGetTaskList)

	router.POST(options.BaseURL+"/api/v1/tasks", wrapper.DMAPIStartTask)

	router.DELETE(options.BaseURL+"/api/v1/tasks/:task-name", wrapper.DMAPIDeleteTask)

	router.POST(options.BaseURL+"/api/v1/tasks/:task-name/pause", wrapper.DMAPIPauseTask)

	router.POST(options.BaseURL+"/api/v1/tasks/:task-name/resume", wrapper.DMAPIResumeTask)

	router.GET(options.BaseURL+"/api/v1/tasks/:task-name/sources/:source-name/schemas", wrapper.DMAPIGetSchemaListByTaskAndSource)

	router.GET(options.BaseURL+"/api/v1/tasks/:task-name/sources/:source-name/schemas/:schema-name", wrapper.DMAPIGetTableListByTaskAndSource)

	router.DELETE(options.BaseURL+"/api/v1/tasks/:task-name/sources/:source-name/schemas/:schema-name/:table-name", wrapper.DMAPIDeleteTableStructure)

	router.GET(options.BaseURL+"/api/v1/tasks/:task-name/sources/:source-name/schemas/:schema-name/:table-name", wrapper.DMAPIGetTableStructure)

	router.PUT(options.BaseURL+"/api/v1/tasks/:task-name/sources/:source-name/schemas/:schema-name/:table-name", wrapper.DMAPIOperateTableStructure)

	router.GET(options.BaseURL+"/api/v1/tasks/:task-name/status", wrapper.DMAPIGetTaskStatus)

	return router
}

// Base64 encoded, gzipped, json marshaled Swagger object
var swaggerSpec = []string{

	"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",
	"kMwuC+bjsgf3n+//HQAA//8aVdY3n30AAA==",
}

// GetSwagger returns the content of the embedded swagger specification file
// or error if failed to decode
func decodeSpec() ([]byte, error) {
	zipped, err := base64.StdEncoding.DecodeString(strings.Join(swaggerSpec, ""))
	if err != nil {
		return nil, fmt.Errorf("error base64 decoding spec: %s", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(zipped))
	if err != nil {
		return nil, fmt.Errorf("error decompressing spec: %s", err)
	}
	var buf bytes.Buffer
	_, err = buf.ReadFrom(zr)
	if err != nil {
		return nil, fmt.Errorf("error decompressing spec: %s", err)
	}

	return buf.Bytes(), nil
}

var rawSpec = decodeSpecCached()

// a naive cached of a decoded swagger spec
func decodeSpecCached() func() ([]byte, error) {
	data, err := decodeSpec()
	return func() ([]byte, error) {
		return data, err
	}
}

// Constructs a synthetic filesystem for resolving external references when loading openapi specifications.
func PathToRawSpec(pathToFile string) map[string]func() ([]byte, error) {
	res := make(map[string]func() ([]byte, error))
	if len(pathToFile) > 0 {
		res[pathToFile] = rawSpec
	}

	return res
}

// GetSwagger returns the Swagger specification corresponding to the generated code
// in this file. The external references of Swagger specification are resolved.
// The logic of resolving external references is tightly connected to "import-mapping" feature.
// Externally referenced files must be embedded in the corresponding golang packages.
// Urls can be supported but this task was out of the scope.
func GetSwagger() (swagger *openapi3.T, err error) {
	resolvePath := PathToRawSpec("")

	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true
	loader.ReadFromURIFunc = func(loader *openapi3.Loader, url *url.URL) ([]byte, error) {
		pathToFile := url.String()
		pathToFile = path.Clean(pathToFile)
		getSpec, ok := resolvePath[pathToFile]
		if !ok {
			err1 := fmt.Errorf("path not found: %s", pathToFile)
			return nil, err1
		}
		return getSpec()
	}
	var specData []byte
	specData, err = rawSpec()
	if err != nil {
		return
	}
	swagger, err = loader.LoadFromData(specData)
	if err != nil {
		return
	}
	return
}
