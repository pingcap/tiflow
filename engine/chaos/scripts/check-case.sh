#!/bin/bash

completed=false
for i in {1..22}; do
	kubectl wait --for=condition=complete job/chaos-test-case --timeout=1m
	if [ $? -eq 0 ]; then
		completed=true
		echo "chaos-test-case has completed"
		break
	else
		echo "chaos-test-case has not completed" ${i}
		kubectl get job chaos-test-case -o wide
		if [ $? -ne 0 ]; then
			echo "chaos-test-case job has been cleared"
			break
		fi
		failed=$(kubectl get job chaos-test-case -o jsonpath={.status.failed})
		if [[ $failed -gt 0 ]]; then
			echo "chaos-test-case job has failed"
			kubectl logs job.batch/chaos-test-case
			kubectl describe jobs/chaos-test-case
			break
		fi
	fi
done

if ! $completed; then
	exit 1
fi
