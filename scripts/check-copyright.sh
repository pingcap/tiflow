excluded_files=("\.pb\.go" "leaktest.go" "kv_gen" "redo_gen" "sink_gen" "pbmock"
	"\.pb\.gw\.go" "statik.go" "openapi/gen\..*\.go" "embedded_asserts.go"
	"empty_asserts.go" "docs/swagger" "bin" "owner/mock" "mock/mockclient.go")
excluded_files_str=""
for elm in "${excluded_files[@]}"; do
	if [ -z "$excluded_files_str" ]; then
		excluded_files_str="${elm}"
	else
		excluded_files_str="${excluded_files_str}|${elm}"
	fi
done
result=$(find ./ -name "*.go" | grep -vE "${excluded_files_str}" |
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
