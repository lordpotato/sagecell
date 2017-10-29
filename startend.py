#! /usr/bin/env python

import json
import psutil
import shlex
import subprocess
import time
import urllib
import urllib2


N = 10


last_error = start = time.time()
subprocess.Popen(shlex.split("../sage/sage web_server.py"))
data = urllib.urlencode(dict(code="print(1+2)", accepted_tos="true"))

while True:
    try:
        request = urllib2.urlopen("http://localhost:8888/service", data)
        reply = json.loads(request.read())
        first_reply = time.time()
        print(reply)
        print("last error was at {}".format(last_error - start))
        print("correct reply obtained at {}, {} later".format(
            first_reply - start, first_reply - last_error))
        for i in range(N):
            request = urllib2.urlopen("http://localhost:8888/service", data)
            reply = json.loads(request.read())
        all_done = time.time()
        print("{} more replies obtained at {}, {} per request".format(
            N, all_done - start, (all_done - first_reply)/N))
        break
    except Exception as e:
        last_error = time.time()
    time.sleep(0.1)

time.sleep(5)   # Let non-participating code work a bit as well
for p in psutil.process_iter(attrs=["pid", "cmdline"]):
    if p.info["cmdline"] == ["python", "web_server.py"]:
        p.terminate()
print("shutdown at {}".format(time.time() - start))
