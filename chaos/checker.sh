#!/bin/bash

# check the job status each 1m for 40 times
completed=false
for i in {1..40}; do
	kubectl wait --for=condition=complete job/bank-workload -n playground --timeout=1m
	if [ $? -eq 0 ]; then
		completed=true
		echo "bank workload has completed"
		break
	else
		echo "bank workload has not completed" ${i}
		kubectl describe job bank-workload -n playground
		if [ $? -ne 0 ]; then
			echo "bank workload job has been cleared"
			break
		fi
		failed=$(kubectl get job bank-workload -n playground -o jsonpath={.status.failed})
		if [[ $failed -gt 0 ]]; then
			echo "bank workload job has failed"
			break
		fi
	fi
done

if ! $completed; then
	exit 1
fi
