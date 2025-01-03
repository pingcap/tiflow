#!/bin/sh

set -ex

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

# create table diff_test.test(`table` int, aa int, b varchar(10), c float, d datetime, primary key(a), key(aa));

sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml

echo "================test bucket checkpoint================="
echo "---------1. chunk is in the last of the bucket---------"
export GO_FAILPOINTS="github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/check-one-bucket=return();\
github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/print-chunk-info=return();\
main/wait-for-checkpoint=return()"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
# Save the last chunk's info, 
# to which we will check whether the first chunk's info is next in the next running.
last_chunk_info=$(grep 'print-chunk-info' $OUT_DIR/sync_diff.log | awk -F 'upperBounds=' '{print $2}' | sed 's/[]["]//g' | sort -n | awk 'END {print}')
echo "$last_chunk_info" # e.g. 537 indexCode=0:0-0:3:4
last_chunk_bound=$(echo $last_chunk_info | awk -F ' ' '{print $1}')
echo "$last_chunk_bound"
last_chunk_index=$(echo $last_chunk_info | awk -F '=' '{print $2}')
echo "$last_chunk_index"
OLD_IFS="$IFS"
IFS=":"
last_chunk_index_array=($last_chunk_index)
IFS="$OLD_IFS"
for s in ${last_chunk_index_array[@]}
do
echo "$s"
done
# chunkIndex should be the last Index
[[ $((${last_chunk_index_array[2]} + 1)) -eq ${last_chunk_index_array[3]} ]] || exit 1
# Save bucketIndexRight, which should be equal to bucketIndexLeft of the chunk first created in the next running. 
bucket_index_right=$(($(echo ${last_chunk_index_array[1]} | awk -F '-' '{print $2}') + 1))
echo $bucket_index_right

rm -f $OUT_DIR/sync_diff.log
export GO_FAILPOINTS="github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/print-chunk-info=return()"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.output
first_chunk_info=$(grep 'print-chunk-info' $OUT_DIR/sync_diff.log | awk -F 'lowerBounds=' '{print $2}' | sed 's/[]["]//g' | sort -n | awk 'NR==1')
echo $first_chunk_info | awk -F '=' '{print $1}' > $OUT_DIR/first_chunk_bound
cat $OUT_DIR/first_chunk_bound
echo $first_chunk_info | awk -F '=' '{print $3}' > $OUT_DIR/first_chunk_index
cat $OUT_DIR/first_chunk_index
# Notice: when chunk is created paralleling, the least chunk may not appear in the first line. so we sort it as before.
check_contains "${last_chunk_bound}" $OUT_DIR/first_chunk_bound
check_contains_regex ".:${bucket_index_right}-.:0:." $OUT_DIR/first_chunk_index

echo "--------2. chunk is in the middle of the bucket--------"
rm -rf $OUT_DIR
mkdir -p $OUT_DIR
export GO_FAILPOINTS="github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/check-one-bucket=return();\
github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/ignore-last-n-chunk-in-bucket=return(1);\
github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/print-chunk-info=return();\
main/wait-for-checkpoint=return()"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
# Save the last chunk's info, 
# to which we will check whether the first chunk's info is next in the next running.
last_chunk_info=$(grep 'print-chunk-info' $OUT_DIR/sync_diff.log | awk -F 'upperBounds=' '{print $2}' | sed 's/[]["]//g' | sort -n | awk 'END {print}')
echo "$last_chunk_info" # e.g. 537 indexCode=0:0-0:3:4
last_chunk_bound=$(echo $last_chunk_info | awk -F ' ' '{print $1}')
echo "$last_chunk_bound"
last_chunk_index=$(echo $last_chunk_info | awk -F '=' '{print $2}')
echo "$last_chunk_index"
OLD_IFS="$IFS"
IFS=":"
last_chunk_index_array=($last_chunk_index)
IFS="$OLD_IFS"
for s in ${last_chunk_index_array[@]}
do
echo "$s"
done
# chunkIndex should be the last Index
[[ $((${last_chunk_index_array[2]} + 2)) -eq ${last_chunk_index_array[3]} ]] || exit 1
# Save bucketIndexRight, which should be equal to bucketIndexLeft of the chunk first created in the next running. 
bucket_index_left=$(echo ${last_chunk_index_array[1]} | awk -F '-' '{print $1}')
bucket_index_right=$(echo ${last_chunk_index_array[1]} | awk -F '-' '{print $2}')
echo "${bucket_index_left}-${bucket_index_right}"

rm -f $OUT_DIR/sync_diff.log
export GO_FAILPOINTS="github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/print-chunk-info=return()"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.output
first_chunk_info=$(grep 'print-chunk-info' $OUT_DIR/sync_diff.log | awk -F 'lowerBounds=' '{print $2}' | sed 's/[]["]//g' | sort -n | awk 'NR==1')
echo $first_chunk_info | awk -F '=' '{print $1}' > $OUT_DIR/first_chunk_bound
cat $OUT_DIR/first_chunk_bound
echo $first_chunk_info | awk -F '=' '{print $3}' > $OUT_DIR/first_chunk_index
cat $OUT_DIR/first_chunk_index
# Notice: when chunk is created paralleling, the least chunk may not appear in the first line. so we sort it as before.
check_contains "${last_chunk_bound}" $OUT_DIR/first_chunk_bound
check_contains_regex ".:${bucket_index_left}-${bucket_index_right}:$((${last_chunk_index_array[2]} + 1)):${last_chunk_index_array[3]}" $OUT_DIR/first_chunk_index


sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base_rand.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml

echo "================test random checkpoint================="
echo "--------------1. chunk is in the middle----------------"
rm -rf $OUT_DIR
mkdir -p $OUT_DIR
export GO_FAILPOINTS="github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/ignore-last-n-chunk-in-bucket=return(1);\
github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/print-chunk-info=return();\
main/wait-for-checkpoint=return()"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
# Save the last chunk's info, 
# to which we will check whether the first chunk's info is next in the next running.
last_chunk_info=$(grep 'print-chunk-info' $OUT_DIR/sync_diff.log | awk -F 'upperBounds=' '{print $2}' | sed 's/[]["]//g' | sort -n | awk 'END {print}')
echo "$last_chunk_info" # e.g. 537 indexCode=0:0-0:3:4
last_chunk_bound=$(echo $last_chunk_info | awk -F ' ' '{print $1}')
echo "$last_chunk_bound"
last_chunk_index=$(echo $last_chunk_info | awk -F '=' '{print $2}')
echo "$last_chunk_index"
OLD_IFS="$IFS"
IFS=":"
last_chunk_index_array=($last_chunk_index)
IFS="$OLD_IFS"
for s in ${last_chunk_index_array[@]}
do
echo "$s"
done
# chunkIndex should be the last Index
[[ $((${last_chunk_index_array[2]} + 2)) -eq ${last_chunk_index_array[3]} ]] || exit 1

rm -f $OUT_DIR/sync_diff.log
export GO_FAILPOINTS="github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/print-chunk-info=return()"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.output
first_chunk_info=$(grep 'print-chunk-info' $OUT_DIR/sync_diff.log | awk -F 'lowerBounds=' '{print $2}' | sed 's/[]["]//g' | sort -n | awk 'NR==1')
echo $first_chunk_info | awk -F '=' '{print $1}' > $OUT_DIR/first_chunk_bound
cat $OUT_DIR/first_chunk_bound
echo $first_chunk_info | awk -F '=' '{print $3}' > $OUT_DIR/first_chunk_index
cat $OUT_DIR/first_chunk_index
# Notice: when chunk is created paralleling, the least chunk may not appear in the first line. so we sort it as before.
check_contains "${last_chunk_bound}" $OUT_DIR/first_chunk_bound
check_contains_regex ".:0-0:$((${last_chunk_index_array[2]} + 1)):${last_chunk_index_array[3]}" $OUT_DIR/first_chunk_index


sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base_continous.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml
echo "================test checkpoint continous================="
# add a table have different table-structs of upstream and downstream
# so data-check will be skipped
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table IF NOT EXISTS diff_test.ttt(a int, aa int, primary key(a), key(aa));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table IF NOT EXISTS diff_test.ttt(a int, b int, primary key(a), key(b));"
export GO_FAILPOINTS="main/wait-for-checkpoint=return()"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.output || true
grep 'save checkpoint' $OUT_DIR/sync_diff.log | awk 'END {print}' > $OUT_DIR/checkpoint_info
check_not_contains 'has-upper\":true' $OUT_DIR/checkpoint_info

export GO_FAILPOINTS=""