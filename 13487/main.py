import os

from helpers import create_pcollection
from helpers import PrefixWrapTableRef
from helpers import PrefixWrapStrRef
from helpers import IssueTester

import apache_beam as beam

from apache_beam.io import BigQueryDisposition

# Testing from env vars
PROJECT = os.environ.get("TMP_BEAM_PROJECT")
DATASET = os.environ.get("TMP_BEAM_DATASET")
LOCATION = os.environ.get("TMP_BEAM_GCS_LOCATION")
REGION = os.environ.get("TMP_BEAM_REGION")

# setup pipeline options
global_opts = beam.pipeline.PipelineOptions(
  runner="DirectRunner",
  project=PROJECT,
  job_name="beam-13487",
  region=REGION,
  temp_location=LOCATION
)

# prepare issue tester class, and build tests
IssueTester.PROJECT = PROJECT
IssueTester.DATASET = DATASET

STAGE_1 = (
  IssueTester(
    "baseline - contains all records",
    "baseline"
  ),
  IssueTester(
    "Fn returns TableRef",
    PrefixWrapTableRef,
    "method1"
  ),
  IssueTester(
    "Fn returns Str",
    PrefixWrapStrRef,
    "method2"
  ),
  IssueTester(
    "Fn returns TableRef - prepare for appends",
    PrefixWrapTableRef,
    "method1_cNever_wAppend"
  ),
  IssueTester(
    "Fn returns Str - prepare for appends",
    PrefixWrapStrRef,
    "method2_cNever_wAppend"
  )
)

STAGE_APPENDS = (
  IssueTester(
    "Fn Returns TableRef, appends no write",
    PrefixWrapTableRef,
    "method1_cNever_wAppend",
    create_disposition=BigQueryDisposition.CREATE_NEVER,
    write_disposition=BigQueryDisposition.WRITE_APPEND
  ),
  IssueTester(
    "Fn Returns Str, appends no write",
    PrefixWrapTableRef,
    "method2_cNever_wAppend",
    create_disposition=BigQueryDisposition.CREATE_NEVER,
    write_disposition=BigQueryDisposition.WRITE_APPEND
  )
)

STAGE_1_NOSCH = (
  IssueTester(
    "baseline - contains all records - nosch",
    "baseline_nosch"
  ),
  IssueTester(
    "Fn returns TableRef - nosch",
    PrefixWrapTableRef,
    "method1_nosch"
  ),
  IssueTester(
    "Fn returns Str - nosch",
    PrefixWrapStrRef,
    "method2_nosch"
  ),
  IssueTester(
    "Fn returns TableRef - prepare for appends - nosch",
    PrefixWrapTableRef,
    "method1_nosch_cNever_wAppend"
  ),
  IssueTester(
    "Fn returns Str - prepare for appends - nosch",
    PrefixWrapStrRef,
    "method2_nosch_cNever_wAppend"
  )
)

STAGE_APPENDS_NOSCH = (
  IssueTester(
    "Fn Returns TableRef, appends no write - nosch",
    PrefixWrapTableRef,
    "method1_nosch_cNever_wAppend",
    create_disposition=BigQueryDisposition.CREATE_NEVER,
    write_disposition=BigQueryDisposition.WRITE_APPEND
  ),
  IssueTester(
    "Fn Returns Str, appends no write - nosch",
    PrefixWrapTableRef,
    "method2_nosch_cNever_wAppend",
    create_disposition=BigQueryDisposition.CREATE_NEVER,
    write_disposition=BigQueryDisposition.WRITE_APPEND
  )
)


def run_stage(stage_list, pcol, sch=None):
  with beam.Pipeline(options=global_opts) as p:
    base = p | pcol
    for t in stage_list:
      if sch:
        t.exec(base, sch)
      else:
        t.exec(base)


if __name__ == "__main__":
  pcol1, sch = create_pcollection(10000, 5)
  pcol2, _ = create_pcollection(10000, 5)
  pcol3, _ = create_pcollection(10000, 5)
  pcol4, _ = create_pcollection(10000, 5)

  run_stage(STAGE_1, pcol1, sch)
  run_stage(STAGE_APPENDS, pcol2, sch)

  run_stage(STAGE_1_NOSCH, pcol3)
  run_stage(STAGE_APPENDS_NOSCH, pcol4)




