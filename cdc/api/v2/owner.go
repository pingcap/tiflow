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

package v2

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// ResignOwner makes the current owner resign
// @Summary Notify the owner to resign
// @Description Notify the current owner to resign
// @Tags owner,v2
// @Accept json
// @Produce json
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/owner/resign [post]
func (h *OpenAPIV2) resignOwner(c *gin.Context) {
	o, _ := h.capture.GetOwner()
	if o != nil {
		o.AsyncStop()
	}

	c.JSON(http.StatusOK, &EmptyResponse{})
}
