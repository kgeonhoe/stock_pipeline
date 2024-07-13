

class Es(object):
  def __init__(self, es_hosts, mode="append", write_operation="overwrite"):
    self.es_hosts = es_hosts
    self.es_mode = mode
    self.es_write_operation = write_operation
    self.es_index_auto_create = "yes"
    # self.es_mapping_id

  def write_df(self, df, es_resource):
    df.write.format("org.elasticsearch.spark.sql") \
      .mode(self.es_mode) \
      .option("es.nodes", self.es_hosts) \
      .option("es.index.auto.create", self.es_index_auto_create) \
      .option("es.resource", es_resource) \
      .save()
    # .option("es.write.operation", self.es_write_operation)
    