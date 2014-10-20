from collections import defaultdict
import simplejson as json
import sys
import traceback

pymap = map

def enum_paths(dct, path=[]):
  if not hasattr(dct, 'items'):
    path.append(dct)
    yield path
    return
  for k,v in dct.iteritems():
    for p in enum_paths(v, path + [k]):
      yield p

def map(k, d, v, cx):
  try:
    j = json.loads(v)
    if not "simpleMeasurements" in j:
      return
    m = j["simpleMeasurements"]
    sysinfo = j["info"]
    if not "UITelemetry" in m:
      return
    ui = m["UITelemetry"]

    ui_payload = {"sysinfo": sysinfo, "uitelemetry": ui}
    cx.write(json.dumps(ui_payload), 1)

  except Exception, e:
    print >> sys.stderr, "ERROR:", e
    print >> sys.stderr, traceback.format_exc()
    cx.write("ERROR:", str(e))

def reduce(k, v, cx):
  if k == "ERROR:":
    for i in set(v):
      cx.write(k, i)
    return

  #cheat and just write the blob. we assume they're not repeated.
  cx.write(k, "")