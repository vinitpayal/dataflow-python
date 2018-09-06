from __future__ import absolute_import
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import SetupOptions
import argparse
import re

DEFAULT_WRITE_FILE = '/tmp/test.txt'

class WordExtractingDoFn(beam.DoFn):
    # def __init__(self):
    #     super(WordExtractingDoFn, self).__init__()

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
                        help='Output file to write results to.')
    parser.add_argument('--execution_type',
                        dest='execution_type',
                        default='file-read',
                        help='Type of execution for this script')

    known_args, pipeline_args = parser.parse_known_args(argv)

    if known_args.execution_type == 'in-memory':
        test_inmemory_word_count()
        return

    if known_args.execution_type == 'in-memory-obj-list':
        test_inmemory_with_obj_list()
        return "success"


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
    counts | 'write' >> WriteToText(known_args.output if known_args.output else DEFAULT_WRITE_FILE)

    result = p.run()
    result.wait_until_finish()


def test_inmemory_word_count(argv=None):
    words_list = ["hey", "hi", "ok", "good", "hey", "nice", "ok", "yo"]
    p = beam.Pipeline()

    parser = argparse.ArgumentParser()
    parser.add_argument('--output',
                        dest='output',
                        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    def print_in_console(word_string_with_count):
        print(word_string_with_count)
        return word_string_with_count

    words = (p
             | 'create pcollection from inmemory' >> beam.Create(words_list)
             | 'pair' >> beam.Map(lambda x: (x, 1))
             | 'grouping' >> beam.GroupByKey()
             | 'count' >> beam.Map(lambda word_count_map: (word_count_map[0], sum(word_count_map[1])))
             | 'format' >> beam.Map(lambda word_result: ('%s: %d' % (word_result[0], word_result[1])))
             )

    # don't store updated pcollection just used for printing
    words | 'console output' >> beam.Map(print_in_console)

    words | 'write' >> WriteToText(known_args.output if known_args.output else DEFAULT_WRITE_FILE)

    result = p.run()
    result.wait_until_finish()


def test_inmemory_with_obj_list():
    words_list = [{"first_name": "vinit", "last_name": "payal"}]
    p = beam.Pipeline()

    def print_pcollection(element):
        print(element["first_name"], element['last_name'])
        return element

    words = (p
             | 'create pcollection from inmemory' >> beam.Create(words_list)
             | 'print pcollection element' >> beam.Map(print_pcollection)
             )

    res = p.run()
    res.wait_until_finish()


run()


