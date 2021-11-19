#!/bin/bash

# this script used to generate tls file

cat - >"ipsan.cnf" <<EOF
[dn]
CN = localhost
[req]
distinguished_name = dn
[EXT]
subjectAltName = @alt_names
keyUsage = digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth,serverAuth
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

openssl ecparam -out "ca.key" -name prime256v1 -genkey
openssl req -new -batch -sha256 -subj '/CN=localhost' -key "ca.key" -out "ca.csr"
openssl x509 -req -sha256 -days 100000 -in "ca.csr" -signkey "ca.key" -out "ca.pem" 2>/dev/null

for role in dm other; do
	openssl ecparam -out "$role.key" -name prime256v1 -genkey
	openssl req -new -batch -sha256 -subj "/CN=${role}" -key "$role.key" -out "$role.csr"
	openssl x509 -req -sha256 -days 100000 -extensions EXT -extfile "ipsan.cnf" -in "$role.csr" -CA "ca.pem" -CAkey "ca.key" -CAcreateserial -out "$role.pem" 2>/dev/null
done
