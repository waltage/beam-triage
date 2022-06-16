import apache_beam as beam

from apache_beam.io.gcp.bigquery import WriteToBigQuery

from apache_beam.io.gcp.internal.clients.bigquery import TableReference
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema

import numpy as np


def create_pcollection(sample_size=10000, n_categories=10):
  if n_categories > 26:
    n_categories = 26
  if n_categories < 2:
    n_categories = 2
  categories = np.random.randint(65, 65 + n_categories, sample_size)
  categories = [chr(_) for _ in categories]
  records = []
  for idx in range(sample_size):
    records.append(dict(
      id=idx,
      category=categories[idx],
      other=(idx + 58391) ** 2 % 62401))
  schema = TableSchema(
    fields=[
      TableFieldSchema(name="id", type="INT64"),
      TableFieldSchema(name="category", type="STRING"),
      TableFieldSchema(name="other", type="INT64")
    ]
  )
  pcollection = "Random Category PCollection" >> beam.Create(records)
  return pcollection, schema


class PrefixWrapTableRef:
  def __init__(self, project, dataset, prefix):
    self.project = project
    self.dataset = dataset
    self.prefix = prefix

  def __call__(self, element):
    table_ref = TableReference(
      projectId=self.project,
      datasetId=self.dataset,
      tableId="13487_table_{}_MISSING".format(self.prefix)
    )
    if "category" in element:
      cat = element["category"]
      table_ref.tableId = "13487_table_{}_{}".format(
        self.prefix, cat
      )
    return table_ref


class PrefixWrapStrRef:
  def __init__(self, project, dataset, prefix):
    self.project = project
    self.dataset = dataset
    self.prefix = prefix

  def __call__(self, element):
    cat = "MISSING"
    if "category" in element:
      cat = element["category"]
    return "{}:{}.13487_table_{}_{}".format(
      self.project, self.dataset, self.prefix, cat
    )


class IssueTester:
  PROJECT = ""
  DATASET = ""

  def __init__(self, test_name, method_or_table, prefix=None, **kwargs):
    self.test_name = test_name
    self.method = method_or_table
    self.prefix = prefix or "_"
    self.kwargs = kwargs

  def exec(self, pipe, schema="SCHEMA_AUTODETECT"):
    if not callable(self.method):
      # it's a string...
      tbl = "{}:{}.13487_table_{}".format(
        self.PROJECT, self.DATASET, self.method)
      pipe | self.test_name + " Write" >> WriteToBigQuery(
        tbl,
        schema=schema,
        **self.kwargs
      )
    else:
      # it's a function
      pipe | self.test_name + " Write" >> WriteToBigQuery(
        self.method(self.PROJECT, self.DATASET, self.prefix),
        schema=schema,
        **self.kwargs
      )
