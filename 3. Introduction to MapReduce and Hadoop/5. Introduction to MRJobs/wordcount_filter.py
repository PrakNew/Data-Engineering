from mrjob.job import MRJob
import re

# Regular expression that finds all the words in a String
WORD_REGEX = re.compile(r"\b\w+\b")


# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    # Our mapper takes a fragment of text as an input and produces a list of (key, value)
    # The key is any line that contains the word 'London', no value is needed
    def mapper(self, _, line):
        if re.search('London', line):
            yield(line, None)


    # We don't need a reducer! This solution follows a FILTER PATTERN 


if __name__ == '__main__':
    MyMapReduce.run()


""" Command:
python wordcount_filter.py input > out.txt
"""

