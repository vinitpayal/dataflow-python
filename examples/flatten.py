import apache_beam as beam
from common import utils


def process_words(element):
    return element

def run():
    words_list = ["hey", "hi", "ok", "good", "hey", "nice", "ok", "yo"]
    names_list = ["vinit", "name1", "name2", "name3"]

    p = beam.Pipeline()

    words = p | 'first pcollection' >> beam.Create(words_list)
    names = p | 'second collection' >> beam.Create(names_list)

    merged = ((words, names) | beam.Flatten())| 'print' >> beam.Map(utils.print_pcollection)

    res = p.run()
    res.wait_until_finished()

