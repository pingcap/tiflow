// Copyright 2024 PingCAP, Inc.
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

package main

import (
	"fmt"
	"net/http"

	"github.com/go-oauth2/oauth2/v4/errors"
	"github.com/go-oauth2/oauth2/v4/generates"
	"github.com/go-oauth2/oauth2/v4/manage"
	"github.com/go-oauth2/oauth2/v4/models"
	"github.com/go-oauth2/oauth2/v4/server"
	"github.com/go-oauth2/oauth2/v4/store"
	"github.com/golang-jwt/jwt"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const openIDConfiguration = `
{
  "issuer": "http://localhost:%d/",
  "authorization_endpoint": "http://localhost:%d/authorize",
  "token_endpoint": "http://localhost:%d/token",
  "scopes_supported": [
    "openid",
    "profile"
  ],
  "response_types_supported": [
    "code",
    "token",
    "id_token",
    "code token",
    "code id_token",
    "token id_token",
    "code token id_token"
  ],
  "code_challenge_methods_supported": [
    "HS256",
    "plain"
  ],
  "response_modes_supported": [
    "query",
    "fragment",
    "form_post"
  ],
  "subject_types_supported": [
    "public"
  ],
  "id_token_signing_alg_values_supported": [
    "HS256"
  ],
  "token_endpoint_auth_methods_supported": [
    "client_secret_basic",
    "client_secret_post",
    "private_key_jwt"
  ],
  "claims_supported": [
    "aud",
    "created_at",
    "email",
    "email_verified",
    "exp",
    "iat",
    "identities",
    "iss",
    "name"
  ],
  "request_uri_parameter_supported": false,
  "request_parameter_supported": false
}
`

type oauth2ServerConfig struct {
	tokenSignSecret string
	clientID        string
	clientSecret    string
	port            int
}

var serverConfig = newServerConfig()

func newServerConfig() *oauth2ServerConfig {
	return &oauth2ServerConfig{}
}

func main() {
	cmd := &cobra.Command{
		Use: "oauth2 server",
		Run: run,
	}
	// Flags for the root command
	cmd.Flags().StringVar(&serverConfig.tokenSignSecret, "token-secret",
		"SJhX36_KapYSybBtJq35lxX_Brr4LRURSkm7QmXJGmy8pUFW9EIOcVQPsykz9-jj", "Secret to sign token")
	cmd.Flags().StringVar(&serverConfig.clientID, "client-id", "1234", "Client ID of oauth2")
	cmd.Flags().StringVar(&serverConfig.clientSecret, "client-secret", "e0KVlA2EiBfjoN13olyZd2kv1KL", "Client secret of oauth2")
	cmd.Flags().IntVar(&serverConfig.port, "log-file", 9096, "log file path")
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
	}
}

func run(_ *cobra.Command, _ []string) {
	manager := manage.NewDefaultManager()
	// token memory store
	manager.MustTokenStorage(store.NewMemoryTokenStore())
	manager.MapAccessGenerate(generates.NewJWTAccessGenerate("", []byte(serverConfig.tokenSignSecret), jwt.SigningMethodHS512))

	// client memory store
	clientStore := store.NewClientStore()
	err := clientStore.Set(serverConfig.clientID, &models.Client{
		ID:     serverConfig.clientID,
		Secret: serverConfig.clientSecret,
		Domain: fmt.Sprintf("http://localhost:%d", serverConfig.port),
	})
	if err != nil {
		log.Panic("set client failed", zap.Error(err))
	}
	manager.MapClientStorage(clientStore)

	srv := server.NewDefaultServer(manager)
	srv.SetAllowGetAccessRequest(true)
	srv.SetClientInfoHandler(server.ClientFormHandler)
	srv.SetInternalErrorHandler(func(err error) (re *errors.Response) {
		log.Error("Internal Error:", zap.Error(err))
		return
	})
	srv.SetResponseErrorHandler(func(re *errors.Response) {
		log.Error("Response Error:", zap.Error(re.Error))
	})
	http.Handle("/authorize", logMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := srv.HandleAuthorizeRequest(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	})))
	http.Handle("/token", logMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := srv.HandleTokenRequest(w, r); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})))
	http.Handle("/.well-known/openid-configuration", logMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(openIDConfiguration, serverConfig.port, serverConfig.port, serverConfig.port)))
		w.WriteHeader(200)
	})))
	log.Info("starting auth2 server", zap.Int("port", serverConfig.port))
	log.Panic("run auth2 server failed", zap.Error(http.ListenAndServe(fmt.Sprintf(":%d", serverConfig.port), nil)))
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		log.Info("oauth server api is called", zap.String("path", r.URL.Path))
	})
}
