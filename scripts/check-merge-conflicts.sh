result=$(git ls-tree --full-tree --name-only -r HEAD | while read file_path; do
    if [[ `grep -E "^<<<<<<< HEAD$" "${file_path}"` ]]; then
        echo "${file_path}"
    fi
done)

if [ -n "$result" ]; then
    echo "Merge conflicts detected:"
    echo "$result"
    exit 1
fi
