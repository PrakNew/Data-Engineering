from mrjob.job import MRJob
import re

# Regular expression that finds all the words in a String
WORD_REGEX = re.compile(r"\b\w+\b")


# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    # Our mapper takes a fragment of text as an input and produces a list of (key, value)
    # The key is the length of the word and the value is 1
    def mapper(self, _, line):
        words = WORD_REGEX.findall(line)
        for word in words:
            yield(len(word), 1)

    # Our reducer takes a group of (key, value) where key = length
    # Where the final value indicates how many times a words with a given length have been read
    def reducer(self, length, counts):
        yield(length, sum(counts))


# 
if __name__ == '__main__':
    MyMapReduce.run()

""" Command:
python wordcount_length.py input > out.txt
"""
