import argparse
import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryWriter
from apache_beam.io.gcp.bigquery import BigQueryWriteFn
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.bigquery import BigQuerySink
from apache_beam import ParDo


def run(argv=None):
    parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     '--input_topic', required=True,
    #     help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')

    known_args, pipeline_args = parser.parse_known_args(argv)

    p = beam.Pipeline(argv=pipeline_args)

    schema = {'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'}]
    }

    # gbq_sink = BigQuerySink('test', dataset='backup_dataset', project='tribes-ai',
    #                         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
    #                         write_disposition=BigQueryDisposition.WRITE_APPEND)

    # write = (p | 'write in GBQ test' >> ParDo(BigQueryWriter(sink=gbq_sink)))

    bigquery_write_fn = BigQueryWriteFn(
        table_id='test',
        dataset_id='backup_dataset',
        project_id='tribes-ai',
        batch_size=100,
        schema=schema,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        client=None
        )

    return p | 'WriteToBigQuery' >> ParDo(bigquery_write_fn)

    res = p.run().wait_until_finished()
