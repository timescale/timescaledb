import csv
import re
import json
import os
import time
import psycopg2

resultlist = list()
resultlist = list(csv.reader(open('tsbsloadoutput.txt', newline=''), delimiter=',', quotechar='"'))
resultlist = resultlist[2:-4]

entry = {}
entry['type'] = 'tsbsload'
#entry['repo'] = os.environ.get('GITHUB_REPOSITORY','missing_GITHUB_REPOSITORY')
entry['repo'] = os.environ.get('DRM_GIT_REPO','missing_DRM_GIT_REPO')
#entry['ref'] = os.environ.get('GITHUB_REF', 'missing_GITHUB_REF')
entry['ref'] = os.environ.get('DRM_GIT_BRANCH', 'missing_DRM_GIT_BRANCH')
#entry['commitid'] = os.environ.get('GITHUB_SHA', 'missing_GITHUB_SHA')
entry['commitid'] = os.environ.get('DRM_GIT_COMMIT', 'missing_DRM_GIT_COMMIT')
entry['cpuinfo'] = os.environ.get('CPU_INFO', 'missing_CPU_INFO')
entry['date'] = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())
entry['entries'] = {}

start = int(resultlist[0][0])
for row in resultlist:
    offset = str(int(row[0]) - start)
    entry['entries'][offset] = {}
    entry['entries'][offset]['rowspersec'] = row[4]

try:
    DB_URL = os.environ.get('DB_URL', 'missing_DB_URL')
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("""
        INSERT into githubruns (datetime, repo, ref, type, commitid, cpuinfo, entry)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """,
        ( 'now()', entry['repo'], entry['ref'], entry['type'], entry['commitid'], entry['cpuinfo'], json.dumps(entry))
        )
    conn.commit()

    cur.close()
    conn.close()

except Exception as e:
    print('Exception!!!')
    print(e)
