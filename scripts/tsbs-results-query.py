import csv
import re
import json
import os
import time
import psycopg2

resultlist = list()
with open('tsbsqueryoutput.txt', newline='') as csvf:
    cread = csv.reader(csvf, delimiter=',', quotechar='"')
    for row in cread:
        if len(row) > 0 and (re.compile('^Run').match(row[0]) or len(resultlist) > 0):
            resultlist.append(row)

query = re.compile('[\w\s]+[\w]')

entry = {}
entry['type'] = query.search(resultlist[3][0]).group()
entry['date'] = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())
#entry['repo'] = os.environ.get('GITHUB_REPOSITORY','missing_GITHUB_REPOSITORY')
entry['repo'] = os.environ.get('DRM_GIT_REPO','missing_DRM_GIT_REPO')
#entry['ref'] = os.environ.get('GITHUB_REF', 'missing_GITHUB_REF')
entry['ref'] = os.environ.get('DRM_GIT_BRANCH', 'missing_DRM_GIT_BRANCH')
#entry['commitid'] = os.environ.get('GITHUB_SHA', 'missing_GITHUB_SHA')
entry['commitid'] = os.environ.get('DRM_GIT_COMMIT', 'missing_DRM_GIT_COMMIT')
entry['cpuinfo'] = os.environ.get('CPU_INFO', 'missing_CPU_INFO')
entry['date'] = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())
entry['entries'] = {}

resultlist = resultlist[4:]

regex = re.compile('\d+[.\d][\d]+')
label = re.compile('\w+')
for row in resultlist:
    for item in row:
        entry[label.search(item).group()] = regex.search(item).group()

try:
    DB_URL = os.environ.get('DB_URL', 'missing_DB_URL')
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute('select count(*) from githubruns')
    conn.commit()
    print(cur.fetchall())

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
