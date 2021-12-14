// Copyright 2021 PingCAP, Inc.
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

package security

import "os"

// All these certificates or keys are use for testing only.
var certPem = `-----BEGIN CERTIFICATE-----
MIIDjzCCAnegAwIBAgIUWBTDQm4xOYDxZBTkpCQouREtT8QwDQYJKoZIhvcNAQEL
BQAwVzELMAkGA1UEBhMCQ04xEDAOBgNVBAgTB0JlaWppbmcxEDAOBgNVBAcTB0Jl
aWppbmcxEDAOBgNVBAoTB1BpbmdDQVAxEjAQBgNVBAMTCU15IG93biBDQTAgFw0y
MDAyMTgwOTExMDBaGA8yMTIwMDEyNTA5MTEwMFowFjEUMBIGA1UEAxMLdGlkYi1z
ZXJ2ZXIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCr2ZxAb+dItEQz
avuza0IoIT/UolC9XTGaQiCUUPZUMN9hb4KYEwTks1ZthHovTIJUdTwHtpWfDUWx
uIXhOlRjfD+viY4aXtBsaK8xi9F7o2HbFQ5O9y3AXK/YW+u0FfWtnn/xAtvPUgUc
61NXtBTMvNard9ICIXW+FWxLcFSaHpC9ZTyr13KWmZRDbai1JFeaKvATMW30r7Dd
Ur1npppzt7ZdG6tU/FuqBBSrZtuVSGKLwVx0JQDw16eVan4friY3ZPNVoZvkKyvt
I1LsBYMQ8p+ijtbcftvMqWZFVw95F1+3C2JIjWN9ujGmvJr+dtPIE8T/J8tT9Jif
9vz16nOLAgMBAAGjgZEwgY4wDgYDVR0PAQH/BAQDAgWgMB0GA1UdJQQWMBQGCCsG
AQUFBwMBBggrBgEFBQcDAjAMBgNVHRMBAf8EAjAAMB0GA1UdDgQWBBRVB/Bvdzvh
6WQRWpc9SzcbXLz77zAfBgNVHSMEGDAWgBSdAhKsS8BKSOidoGCUYNeaFma4/zAP
BgNVHREECDAGhwR/AAABMA0GCSqGSIb3DQEBCwUAA4IBAQAAqg5pgGQqORKRSdlY
wzVvzKaulpvjZfVMM6YiOUtmlU0CGWq7E3gLFzkvebpU0KsFlbyZ92h/2Fw5Ay2b
kxkCy18mJ4lGkvF0cU4UD3XheFMvD2QWWRX4WPpAhStofrWOXeyq3Div2+fQjMJd
kyeWUzPU7T467IWUHOWNsFAjfVHNsmG45qLGt+XQckHTvASX5IvN+5tkRUCW30vO
b3BdDQUFglGTUFU2epaZGTti0SYiRiY+9R3zFWX4uBcEBYhk9e/0BU8FqdWW5GjI
pFpH9t64CjKIdRQXpIn4cogK/GwyuRuDPV/RkMjrIqOi7pGejXwyDe9avHFVR6re
oowA
-----END CERTIFICATE-----`

var caPem = `-----BEGIN CERTIFICATE-----
MIIDgDCCAmigAwIBAgIUHWvlRJydvYTR0ot3b8f6IlSHcGUwDQYJKoZIhvcNAQEL
BQAwVzELMAkGA1UEBhMCQ04xEDAOBgNVBAgTB0JlaWppbmcxEDAOBgNVBAcTB0Jl
aWppbmcxEDAOBgNVBAoTB1BpbmdDQVAxEjAQBgNVBAMTCU15IG93biBDQTAgFw0y
MDAyMTgwNzQxMDBaGA8yMTIwMDEyNTA3NDEwMFowVzELMAkGA1UEBhMCQ04xEDAO
BgNVBAgTB0JlaWppbmcxEDAOBgNVBAcTB0JlaWppbmcxEDAOBgNVBAoTB1BpbmdD
QVAxEjAQBgNVBAMTCU15IG93biBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAOAdNtjanFhPaKJHQjr7+h/Cpps5bLc6S1vmgi/EIi9PKv3eyDgtlW1r
As2sjXRMHjcuZp2hHJ9r9FrMQD1rQQq5vJzQqM+eyWLc2tyZWXNWkZVvpjU4Hy5k
jZFLXoyHgAvps/LGu81F5Lk5CvLHswWTyGQUCFi1l/cYcQg6AExh2pO/WJu4hQhe
1mBBIKsJhZ5b5tWruLeI+YIjD1oo1ADMHYLK1BHON2fUmUHRGbrYKu4yCuyip3wn
rbVlpabn7l1JBMatCUJLHR6VWQ2MNjrOXAEUYm4xGEN+tUYyUOGl5mHFguLl3OIn
wj+1dT3WOr/NraPYlwVOnAd9GNbPJj0CAwEAAaNCMEAwDgYDVR0PAQH/BAQDAgEG
MA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYEFJ0CEqxLwEpI6J2gYJRg15oWZrj/
MA0GCSqGSIb3DQEBCwUAA4IBAQCf8xRf7q1xAaGrc9HCPvN4OFkxDwz1CifrvrLR
ZgIWGUdCHDW2D1IiWKZQWeJKC1otA5x0hrS5kEGfkLFhADEU4txwp70DQaBArPti
pSgheIEbaT0H3BUTYSgS3VL2HjxN5OVMN6jNG3rWyxnJpNOCsJhhJXPK50CRZ7fk
Dcodj6FfEM2bfp2bGkxyVtUch7eepfUVbslXa7jE7Y8M3cr9NoLUcSP6D1RJWkNd
dBQoUsb6Ckq27ozEKOgwuBVv4BrrbFN//+7WHP8Vy6sSMyd+dJLBi6wehJjQhIz6
vqLWE81rSJuxZqjLpCkFdeEF+9SRjWegU0ZDM4V+YeX53BPC
-----END CERTIFICATE-----
`

var keyPem = `-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAq9mcQG/nSLREM2r7s2tCKCE/1KJQvV0xmkIglFD2VDDfYW+C
mBME5LNWbYR6L0yCVHU8B7aVnw1FsbiF4TpUY3w/r4mOGl7QbGivMYvRe6Nh2xUO
TvctwFyv2FvrtBX1rZ5/8QLbz1IFHOtTV7QUzLzWq3fSAiF1vhVsS3BUmh6QvWU8
q9dylpmUQ22otSRXmirwEzFt9K+w3VK9Z6aac7e2XRurVPxbqgQUq2bblUhii8Fc
dCUA8NenlWp+H64mN2TzVaGb5Csr7SNS7AWDEPKfoo7W3H7bzKlmRVcPeRdftwti
SI1jfboxprya/nbTyBPE/yfLU/SYn/b89epziwIDAQABAoIBACPlI08OULgN90Tq
LsLuP3ZUY5nNgaHcKnU3JMj2FE3Hm5ElkpijOF1w3Dep+T+R8pMjnbNavuvnAMy7
ZzOBVIknNcI7sDPv5AcQ4q8trkbt/I2fW0rBNIw+j/hYUuZdw+BNABpeZ31pe2nr
+Y+TLNkLBKfyMiqBxK88mE81mmZKblyvXCawW0A/iDDJ7fPNqoGF+y9ylTYaNRPk
aJGnaEZobJ4Lm5tSqW4gRX2ft6Hm67RkvVaopPFnlkvfusXUTFUqEVQCURRUqXbf
1ah2chUHxj22UdY9540H5yVNgEP3oR+uS/hbZqxKcJUTznUW5th3CyQPIKMlGlcB
p+zWlTECgYEAxlY4zGJw4QQwGYMKLyWCSHUgKYrKu2Ub2JKJFMTdsoj9H7DI+WHf
lQaO9NCOo2lt0ofYM1MzEpI5Cl/aMrPw+mwquBbxWdMHXK2eSsUQOVo9HtUjgK2t
J2AYFCfsYndo+hCj3ApMHgiY3sghTCXeycvT52bm11VeNVcs3pKxIYMCgYEA3dAJ
PwIfAB8t+6JCP2yYH4ExNjoMNYMdXqhz4vt3UGwgskRqTW6qdd9JvrRQ/JPvGpDy
T375h/+lLw0E4ljsnOPGSzbXNf4bYRHTwPOL+LqVM4Bg90hjclqphElHChxep1di
WcdArB0oae/l4M96z3GjfnXIUVOp8K6BUQCab1kCgYAFFAQUR5j4SfEpVg+WsXEq
hcUzCxixv5785pOX8opynctNWmtq5zSgTjCu2AAu8u4a69t/ROwT16aaO2YM0kqj
Ps3BNOUtFZgkqVVaOL13mnXiKjbkfo3majFzoqoMw13uuSpY4fKc+j9fxOQFXRrd
M9jTHfFfJhJpbzf44uyiHQKBgFIPwzvyVvG+l05/Ky83x9fv/frn4thxV45LmAQj
sHKqbjZFpWZcSOgu4aOSJlwrhsw3T84lVcAAzmXn1STAbVll01jEQz6QciSpacP6
1pAAx240UqtptpD6BbkROxz8ffA/Hf3E/6Itb2QyAsP3PqI8kpYYkTG1WCvZA7Kq
HHiRAoGAXbUZ25LcrmyuxKWpbty8fck1tjKPvclQB35rOx6vgnfW6pcKMeebYvgq
nJka/QunEReOH/kGxAd/+ymvUBuFQCfFg3Aus+DtAuh9AkBr+cIyPjJqynnIT87J
MbkOw4uEhDJAtGUR9o1j83N1f05bnEwssXiXR0LZPylb9Qzc4tg=
-----END RSA PRIVATE KEY-----
`

// NewCredential4Test return a Credential for testing
func NewCredential4Test(cn string) (Credential, error) {
	res := Credential{}
	if cn != "" {
		res.CertAllowedCN = append(res.CertAllowedCN, cn)
	}
	cert, err := os.CreateTemp("", "ticdc-test-cert")
	if err != nil {
		return res, err
	}
	_, err = cert.Write([]byte(certPem))
	if err != nil {
		return res, err
	}

	ca, err := os.CreateTemp("", "ticdc-test-ca")
	if err != nil {
		return res, err
	}
	_, err = ca.Write([]byte(caPem))
	if err != nil {
		return res, err
	}

	key, err := os.CreateTemp("", "ticdc-test-key")
	if err != nil {
		return res, err
	}
	_, err = key.Write([]byte(keyPem))
	if err != nil {
		return res, err
	}

	res.CertPath = cert.Name()
	res.CAPath = ca.Name()
	res.KeyPath = key.Name()

	return res, nil
}
