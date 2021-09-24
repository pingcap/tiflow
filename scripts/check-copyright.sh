result=$(find ./ -name "*.go" | grep -vE '.pb.go|vendor/|leaktest.go|kv_gen' | while read file_path; do
	head=$(cat "${file_path}" | head -n 1)
	if [[ ! "$head" =~ Copyright\ 20[0-9][0-9]\ PingCAP,\ Inc\. ]]; then
		echo "${file_path}"
	fi
done)

if [ -n "$result" ]; then
	echo "The copyright information of following files is incorrect:"
	echo "$result"
	exit 1
fi
