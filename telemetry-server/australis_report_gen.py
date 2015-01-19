#!/usr/bin/python

import argparse
import simplejson as json
import os
from collections import namedtuple
import traceback
import sys
import telemetry.util.timer as timer
from analysis.process_output import process_output
from datetime import datetime, timedelta
from urllib import urlopen
import subprocess

from mapreduce.job import Job
try:
    from boto.s3.connection import S3Connection
    BOTO_AVAILABLE=True
except ImportError:
    BOTO_AVAILABLE=False

OUTPUT_DIR_BASE = "../output"

#weeks start at 1
def get_week_endpoints(week_no, year):
  year_start = datetime(year,1,1).date()
  first_tues = year_start + timedelta(days=((8 - year_start.weekday()) % 7))

  if week_no:
    start = first_tues + timedelta(days=7*(int(week_no)-1))
    end = start + timedelta(days=6) #endpoints are inclusive

  elif week_no == "current": #return previous complete week. If today is tuesday, takes previous week
    last_full_day = datetime.today().date() - timedelta(days=1)
    last_full_mon = last_full_day - timedelta(days=(last_full_day.weekday()%7))
    end = last_full_mon #end on a monday!
    start = end - timedelta(days=6)

  else:
    return("00000000", "99999999")

  return (start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))

def get_last_completed_week():
  #TODO: special case for beginning of year

  today = datetime.today()
  year = today.year
  year_start = datetime(year,1,1)

  first_tues = year_start + timedelta(days=((8 - year_start.weekday()) % 7))
  return (year, int((today - first_tues).days / 7))

def corr_version(channel, date):
  if channel == "nightly":
    channel = "central"
  start_date = datetime.strptime(date, "%Y%m%d").date()
  releases_site = urlopen("http://latte.ca/cgi-bin/status.cgi")
  releases = json.loads(releases_site.read())

  for i,r in enumerate(releases):
    if datetime.strptime(r["sDate"], "%Y-%m-%d").date() > start_date:
      if i == 0:
        raise Exception("date too early for dataset")
      return releases[i-1]["data"][channel].split()[1]
  raise Exception("today's date too late for dataset")



#many fields faked out. keep this way for now.
def generate_filters(args, output_file):

    start, end = get_week_endpoints(args.week, args.year) 

    #potentially add more precision later  
    version_min = str(args.version) + ".0"
    version_max = str(args.version) + ".999"

    fltr = {
    "version": 1,
    "dimensions": [
    {
      "field_name": "reason",
      "allowed_values": ["saved-session"]
    },
    {
      "field_name": "appName",
      "allowed_values": ["Firefox"]
    },
    {
      "field_name": "appUpdateChannel",
      "allowed_values": args.channel
    },
    {
      "field_name": "appVersion",
      "allowed_values": {
        "min": str(version_min),
        "max": str(version_max)
      }
    },
    {
      "field_name": "appBuildID",
      "allowed_values": {
        "min": "0",
        "max": "99999999999999"
      }
    },
    {
      "field_name": "submission_date",
      "allowed_values": {
        "min": start,
        "max": end
      }
    }
    ]
    }
    with open(output_file, "w") as outfile:
        json.dump(fltr, outfile)

    return output_file

#many of these args can be exposed at the command line. no need for now.
def run_mr(filter, output_file, local_only, streaming):

  args = {
    "job_script" : "../bucketless_uitour.py",
    "input_filter": filter,
    "num_mappers" : 16,
    "num_reducers" : 4,
    "data_dir" : "../work/cache",
    "work_dir" : "../work",
    "output" : output_file,
    "bucket" : "telemetry-published-v2",
    "local_only" : local_only,
    "delete_data" : streaming
  }

  if not args["local_only"]:
      if not BOTO_AVAILABLE:
          print "ERROR: The 'boto' library is required except in 'local-only' mode."
          print "       You can install it using `sudo pip install boto`"
          parser.print_help()
          return -2

  job = Job(args)
  start = datetime.now()
  exit_code = 0
  try:
      job.mapreduce()
  except:
      traceback.print_exc(file=sys.stderr)
      exit_code = 2
  duration = timer.delta_sec(start)
  print "All done in %.2fs" % (duration)
  return (exit_code, output_file)


#TODO: add ability to specify buildid
parser = argparse.ArgumentParser()
parser.add_argument("-w", "--week", type=int, help="enter week number of year to analyze")
parser.add_argument("-y", "--year", type=int, help="enter year to correspond to week number")
parser.add_argument("-c", "--channel", help ="enter channel to analyze")
parser.add_argument("-v", "--version", help="enter version")
parser.add_argument("-t", "--tag", help="enter a label to identify the data run")
parser.add_argument("--local-only", action="store_true", dest="local_only", help="use flag to run using local data")
parser.add_argument("--most-recent", action="store_true", dest="most_recent", help="get data for most recent week and year. overrides other date and version options")
parser.add_argument("--streaming", action="store_true", dest="streaming", help="use flag to delete files as they're read for more efficient processing")


args = parser.parse_args()

current_dir = sys.path[0]

#TODO: change printed errors to actual raises
if args.channel not in ["nightly", "aurora", "beta", "release"]:
  print "ERROR: channel must be one of (nightly, aurora, beta, release)"

if args.most_recent:
  (args.year, args.week) = get_last_completed_week()
  args.version = corr_version(args.channel, get_week_endpoints(args.week, args.year)[0])

else:
  if not args.week or not args.year or not args.version:
    print "ERROR: must specify week, year, and version"
  if args.version == "current":
    args.version = corr_version(args.channel, get_week_endpoints(args.week, args.year)[0])

start_date = get_week_endpoints(args.week, args.year)[0]
#must be no larger than single week
if not args.tag:
  args.tag = "week_of_" + start_date + "_" + str(args.channel)

output_dir = "/".join([current_dir,OUTPUT_DIR_BASE,args.tag]) + "/"
proc = subprocess.Popen(["mkdir","/".join([current_dir,OUTPUT_DIR_BASE,args.tag])])
proc.wait()

filterfile = generate_filters(args, output_dir + "filter.json")
error, mr_file = run_mr(filterfile, output_dir + "mr_output.csv", args.local_only, args.streaming)
print output_dir + "../" + args.tag + ".csv"
process_output(mr_file, output_dir + "../" + args.tag + ".csv")
