#!/bin/bash

# The URL of your scrapyd instance
SCRAPYD_URL="http://localhost:6800"

echo "Fetching running jobs from $SCRAPYD_URL..."

# 1. Get the list of running jobs and parse it with jq.
# 2. The output is formatted as "project_name job_id" for each running job.
# 3. Pipe this list into a 'while' loop.
curl -s "$SCRAPYD_URL/listjobs.json" | jq -r '.running[], .pending[] | "\(.project) \(.id)"' | \
while read -r project job_id; do
  if [ -n "$project" ] && [ -n "$job_id" ]; then
    # For each job, send a cancel request.
    echo "Stopping job $job_id in project $project..."
    curl -s "$SCRAPYD_URL/cancel.json" -d project="$project" -d job="$job_id"
  fi
done

echo "Done. All running jobs have been sent a stop signal."
