result=$(find ./ -name "*.go" | grep -vE '\.pb\.go|vendor/|leaktest.go|kv_gen|redo_gen|sink_gen|pbmock|\.pb\.gw\.go|statik.go|openapi/gen\..*\.go|_mock\.go|embedded_asserts.go|empty_asserts.go|docs/swagger|bin|owner/mock' |
	while read -r file_path; do
		head=$(head -n 1 "$file_path")
		if [[ ! "$head" =~ Copyright\ 20[0-9][0-9]\ PingCAP,\ Inc\. ]]; then
			echo "${file_path}"
		fi
	done)

if [ -n "$result" ]; then
	echo "The copyright information of following files is incorrect:"
	echo "$result"
	exit 1
fi
