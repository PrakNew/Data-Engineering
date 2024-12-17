from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# Regular expression that finds all the words in a String
WORD_REGEX = re.compile(r"\b\w+\b")


# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    def steps(self):
        return  [
            MRStep (mapper=self.mapper,
                    reducer=self.reducer),
            MRStep(reducer=self.sort_by_length)
            ]


    # Our mapper takes a fragment of text as an input and produces a list of (key, value), where value is the length
    # only if the length is greater than a predefined value (14)
    def mapper(self, _, line):
        words = WORD_REGEX.findall(line)
        for word in words:
            if len(word) > 14:
                yield(word, len(word))

    # The reducer produces only one instance of each word
    def reducer(self, word, length):
        yield None, (len(word), word)

    def sort_by_length(self, _,pair):
        sorted_pairs = sorted(pair, reverse = True)
        for pair in sorted_pairs:
            yield pair


if __name__ == '__main__':
    MyMapReduce.run()

""" Command:
python wordcount_longest_sorted.py inputFull > out.txt

"""