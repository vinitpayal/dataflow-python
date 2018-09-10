import apache_beam as beam
from apache_beam import window
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from common import utils
import argparse


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic', required=True,
        help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')

    known_args, pipeline_args = parser.parse_known_args(argv)

    p = beam.Pipeline(argv=pipeline_args)

    lines = (p
            | ReadFromPubSub(known_args.input_topic)
            |'window' >> beam.WindowInto(window.FixedWindows(2))
            | 'print pubsub messages' >> beam.Map(utils.print_pcollection))

    result = p.run()
    result.wait_until_finish()

