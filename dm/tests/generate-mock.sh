#!/bin/bash

set -eu

cd "$(dirname "${BASH_SOURCE[0]}")"/../..

PACKAGE="pbmock"
MOCKGEN="tools/bin/mockgen"

if [ ! -f "$MOCKGEN" ]; then
	echo "${MOCKGEN} does not exist, please run 'make tools/bin/mockgen' first"
	exit 1
fi

echo "generate grpc mock code..."

for file in ./dm/dm/pb/*pb.go; do
	prefix=$(echo "$file" | awk -F"/" '{print $(NF)}' | awk -F"." '{print $1}')
	# extract public interface from pb source file
	ifs=$(grep -E "type [[:upper:]].*interface" "$file" | awk '{print $2}' 'ORS=,' | rev | cut -c 2- | rev)
	echo "generate mock for file $file"
	"$MOCKGEN" -destination ./dm/dm/pbmock/"$prefix".go -package "$PACKAGE" github.com/pingcap/tiflow/dm/dm/pb "$ifs"
done

echo "generate grpc mock code successfully"
