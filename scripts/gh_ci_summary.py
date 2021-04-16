#!/usr/bin/python3

from datetime import datetime
import requests
import json
import os

# API reference: https://docs.github.com/en/rest/reference/actions#workflow-runs
# Slack message formatting: https://api.slack.com/reference/surfaces/formatting
url = 'https://api.github.com/repos/timescale/timescaledb/actions/runs?event=schedule&status=completed'

message=list()

def get_json(url):
  response = requests.get(url)
  return response.json()

# get runs from last 24 hours
def process_runs(runs):
  failed=list()

  for run in runs:
    start = datetime.strptime(run['created_at'], "%Y-%m-%dT%H:%M:%SZ")
    delta = datetime.now() - start

    if delta.days >= 1:
      break

    if run['conclusion'] != 'success':
      failed.append(run)

  return failed

def print_run_details(run):
  job_data = get_json(run['jobs_url'])
  for job in job_data['jobs']:
    if job['conclusion'] != 'success':
      message.append("<{html_url}|{workflow_name} {name}>".format(workflow_name=run['name'], **job))

def print_summary(failed):
  if len(failed) > 0:
    message.append("Failed scheduled CI runs in last 24 hours:")
    for run in failed:
      print_run_details(run)
  else:
    message.append("No failed scheduled CI runs in last 24 hours :tada:")

try:
  data = get_json(url)
  failed = process_runs(data['workflow_runs'])
  print_summary(failed)
except json.decoder.JSONDecodeError:
  message.append("Error processing GitHub response")

slack_msg=dict()
slack_msg['channel'] = os.environ['SLACK_CHANNEL']
slack_msg['text'] = "\n".join(message)

print(json.dumps(slack_msg))
