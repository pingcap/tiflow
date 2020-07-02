copyright="// Copyright $(date '+%Y') PingCAP, Inc."

result=$(find ./ -name "*.go" | grep -vE '.pb.go|vendor/' | while read file_path; do
    head=`cat "${file_path}" | head -n 1`
    if [ "$head" != "$copyright" ];then
        echo "${file_path}"
    fi
done)

if [ -n "$result" ]; then
    echo "The copyright information of following files is incorrect:"
    echo "$result"
    exit 1
fi
