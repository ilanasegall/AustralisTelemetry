from collections import defaultdict
import json
import sys
import traceback
from cStringIO import StringIO
import os
import logging

logging.basicConfig(filename=os.environ["OUTPUTDIR"]+'warning.log',level=logging.DEBUG)

pymap = map
BYSESSION =True if "BYSESSION" in os.environ else False
#if bysession is true, we only want to care about search data. ow we're going to crash big time.


def enum_paths(dct, path=[]):
  if not hasattr(dct, 'items'):
    path.append(dct)
    yield path
    return
  for k,v in dct.iteritems():
    for p in enum_paths(v, path + [k]):
      yield p

def map(k, d, v, cx):
  payload_out = []
  try:
    j = json.loads(v)
    if not "simpleMeasurements" in j:
      return
    s = j["simpleMeasurements"]
    if not "UITelemetry" in s:
      return
    sysinfo = j["info"]
    locale = sysinfo["locale"]
    if BYSESSION and locale != "en-US":
      return #very hard to look at default search engines, etc, for non en-us. remove later.

    ui = s["UITelemetry"]
    countableEventBuckets = []
    customizeBuckets = []

    prefix = sysinfo["OS"]
    if BYSESSION:
      if not "clientID" in j:
        logging.warning("no clientID")
        return
      if j["clientID"][-1] not in ["1","2"]: #reduce the amount of output so that we don't run out of memory
        return
      prefix +=  "," + j["clientID"]   

    s_prefix = prefix.encode('utf-8')   

    tour_seen = "none"

    if "toolbars" not in ui:
      logging.warning("no toolbars entry")
      return
    toolbars = ui["toolbars"] 
    if not "menuBarEnabled" in toolbars: #remove weird incomplete cases
      return
    countableEvents = toolbars.get("countableEvents", {})
    feature_measures = {}
    #note: simple swaps in "kept"
    feature_measures["features_kept"] = toolbars.get("defaultKept",[])
    feature_measures["features_moved"] = toolbars.get("defaultMoved",[])
    feature_measures["extra_features_added"] = toolbars.get("nondefaultAdded", [])
    feature_measures["features_removed"] = toolbars.get("defaultRemoved", [])
  
    if "UITour" in ui:
      for tour in ui["UITour"]["seenPageIDs"]:
        tour_seen = tour
        payload_out.append((s_prefix + "," + "seenPage-" + tour.encode('utf-8'), 1))
        #TODO: error checking on more than one tour

    for k,v in toolbars.iteritems():
      if k not in ["defaultKept", "defaultMoved", "nondefaultAdded", "defaultRemoved", "countableEvents", "durations"]:
        if not isinstance(v, unicode):
          str_v = str(v)
        else:
          str_v = v.encode("utf-8")
        str_v = str_v.replace(",", " ") #remove commas in the tab arrays that will mess us up later
        payload_out.append((s_prefix + ","+ k.encode('utf-8') + "-" + str_v.strip(), 1)) #strip() because searchengine names sometimes have crap attached

    bucketDurations = defaultdict(list)
    durations = toolbars.get("durations",{}).get("customization",[])

    for e in durations:
        #correct for addition of firstrun buckets
        if type(e) is dict:
          bucketDurations[e["bucket"]].append(e["duration"])
        #if the default bucket is "__DEFAULT__", no tour has been seen
        else:
          bucketDurations["none"].append(e)


    for d,l in bucketDurations.items():
        bucket = "none" if d == "__DEFAULT__" else d
        for i in l:
          payload_out.append((s_prefix + ","+ "customization_time", int(round(float(i)/1000))))

    #record the locations and movement of the customization items
    #write out entire set for a user(dist), 
    #and also each individual item
    for e,v in feature_measures.items():
        for item in v:
          payload_out.append((s_prefix + "," + e+"-"+item.encode('utf-8'), 1))

    #this will break pre-Australis
    bucketless_events = defaultdict(int)
    for i in countableEvents.values():
      for event_string in enum_paths(i,[]):
        bucketless_events["-".join(event_string[0:-1])] += int(event_string[-1])


    for event_string,val in bucketless_events.items():
      payload_out.append((s_prefix + "," + event_string.encode('utf-8'), val))



    #We haven't errored out! Now we can write everything.

    cx.write(prefix+ ",instances", 1)
    for tup in payload_out:
      a,b = tup
      cx.write(a,b)    

  except Exception, e:
    print >> sys.stderr, "ERROR:", e
    print >> sys.stderr, traceback.format_exc()
    cx.write("ERROR:", str(e))

def distn(lst):
  dist = defaultdict(int)
  for i in lst:
    dist[i] += 1
  return dict(dist)

def reduce(k, v, cx):
  if k == "JSON PARSE ERROR:":
    for i in set(v):
      cx.write(k, i)
    return
  try:
    if BYSESSION and "search" not in k.lower():
      return #too much info right now. we'll run out of memory
    cx.write(k, json.dumps(distn(v)))
  except Exception, e:
    print >> sys.stderr, "ERROR:", e