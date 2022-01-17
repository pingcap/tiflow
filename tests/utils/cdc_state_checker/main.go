// Copyright 2020 PingCAP, Inc.
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
	"context"
	"flag"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	pd            = flag.String("pd", "http://127.0.0.1:2379", "PD address and port")
	caPath        = flag.String("ca", "", "CA certificate path for TLS connection")
	certPath      = flag.String("cert", "", "Certificate path for TLS connection")
	keyPath       = flag.String("key", "", "Private key path for TLS connection")
	allowedCertCN = flag.String("cert-allowed-cn", "", "Verify caller's identity "+
		"(cert Common Name). Use `,` to separate multiple CN")
)

func main() {
	flag.Parse()
	log.SetLevel(zapcore.DebugLevel)

	cdcMonitor, err := newCDCMonitor(context.TODO(), *pd, getCredential())
	if err != nil {
		log.Panic("Error creating CDCMonitor", zap.Error(err))
	}

	err = cdcMonitor.run(context.TODO())
	log.Panic("cdcMonitor exited", zap.Error(err))
}

func getCredential() *security.Credential {
	var certAllowedCN []string
	if len(*allowedCertCN) != 0 {
		certAllowedCN = strings.Split(*allowedCertCN, ",")
	}
	return &security.Credential{
		CAPath:        *caPath,
		CertPath:      *certPath,
		KeyPath:       *keyPath,
		CertAllowedCN: certAllowedCN,
	}
}
