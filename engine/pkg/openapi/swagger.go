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

// BSD 3-Clause License
//
// Copyright (c) 2020, Juan Font
// All rights reserved.

// This file is almost a copy of
// https://github.com/juanfont/headscale/blob/19455399f4762fb0ab1308a78f4625ce746b2c51/swagger.go.
// The only difference is the apiV1JSON variable.

package openapi

import (
	"bytes"
	_ "embed"
	"html/template"
	"net/http"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

//go:embed apiv1.swagger.json
var apiV1JSON []byte

// SwaggerUI renders the swagger ui.
func SwaggerUI(
	writer http.ResponseWriter,
	_ *http.Request,
) {
	swaggerTemplate := template.Must(template.New("swagger").Parse(`
<html>
	<head>
	<link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@3/swagger-ui.css">

	<script src="https://unpkg.com/swagger-ui-dist@3/swagger-ui-standalone-preset.js"></script>
	<script src="https://unpkg.com/swagger-ui-dist@3/swagger-ui-bundle.js" charset="UTF-8"></script>
	</head>
	<body>
	<div id="swagger-ui"></div>
	<script>
		window.addEventListener('load', (event) => {
			const ui = SwaggerUIBundle({
			    url: "/swagger/v1/openapiv2.json",
			    dom_id: '#swagger-ui',
			    presets: [
			      SwaggerUIBundle.presets.apis,
			      SwaggerUIBundle.SwaggerUIStandalonePreset
			    ],
				plugins: [
                	SwaggerUIBundle.plugins.DownloadUrl
            	],
				deepLinking: true,
				// TODO(kradalby): Figure out why this does not work
				// layout: "StandaloneLayout",
			  })
			window.ui = ui
		});
	</script>
	</body>
</html>`))

	var payload bytes.Buffer
	if err := swaggerTemplate.Execute(&payload, struct{}{}); err != nil {
		log.Warn("Could not render Swagger", zap.Error(err))

		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusInternalServerError)
		_, err := writer.Write([]byte("Could not render Swagger"))
		log.Warn("Failed to write response", zap.Error(err))

		return
	}

	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, err := writer.Write(payload.Bytes())
	if err != nil {
		log.Warn("Failed to write response", zap.Error(err))
	}
}

// SwaggerAPIv1 serves the apiv1 swagger json file.
func SwaggerAPIv1(
	writer http.ResponseWriter,
	_ *http.Request,
) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	if _, err := writer.Write(apiV1JSON); err != nil {
		log.Warn("Failed to write response", zap.Error(err))
	}
}
