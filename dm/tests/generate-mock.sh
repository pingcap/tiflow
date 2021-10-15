#!/bin/bash

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
package="pbmock"

which $cur/../tools/bin/mockgen >/dev/null
if [ "$?" != 0 ]; then
	echo "'mockgen' does not exist, please run 'make tools_setup' first"
	exit 1
fi

echo "generate grpc mock code..."

cd $cur/../dm/pb
for file in ./*pb.go; do
	prefix=$(echo $file | awk -F"/" '{print $(NF)}' | awk -F"." '{print $1}')
	# extract public interface from pb source file
	ifs=$(grep -E "type [[:upper:]].*interface" $file | awk '{print $2}' 'ORS=,' | rev | cut -c 2- | rev)
	echo "generate mock for file $file"
	$cur/../tools/bin/mockgen -destination $cur/../dm/pbmock/$prefix.go -package $package github.com/pingcap/dm/dm/pb $ifs
done

echo "generate grpc mock code successfully"
