from __future__ import absolute_import

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.metrics import Metrics
from apache_beam.pipeline import PCollection
from apache_beam.pipeline import SetupOptions
import argparse
import re


class WordExtractingDoFn(beam.DoFn):
    def __init__(self):
        super(WordExtractingDoFn, self).__init__()

    def process(self, element):
        text_line = element.strip()

        words = re.findall(r'[\w\']+', text_line, re.UNICODE)

        return words


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='/Users/vinit/Documents/test.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    def count_ones(word_ones):
        (word, ones) = word_ones
        return (word, sum(ones))

    def format_result(word_count):
        (word, count) = word_count
        print '%s: %d' % (word, count)
        return '%s: %d' % (word, count)

    lines = p | 'read' >> ReadFromText(known_args.input)

    counts = (lines
              | (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
              | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
              | 'group' >> beam.GroupByKey()
              | 'count' >> beam.Map(count_ones)
              | 'format' >> beam.Map(format_result)
              )
    counts | 'write' >> WriteToText(known_args.output)

    result = p.run()
    result.wait_until_finish()


run()
