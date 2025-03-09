from mrjob.job import MRJob
from mrjob.step import MRStep

class PaymentTypesCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort_reducer)
        ]

    def mapper(self, _, line):
        # Skip the header row
        if not line.startswith('VendorID'):
            try:
                # Extract payment type (assuming column 9 is payment_type)
                data = line.strip().split(',')
                payment_type = data[9].strip()
                yield payment_type, 1
            except IndexError:
                pass  # Skip malformed lines

    def reducer(self, key, values):
        # Sum up the occurrences of each payment type
        yield None, (sum(values), key)

    def sort_reducer(self, _, values):
        # Sort by count in descending order and output
        for count, payment_type in sorted(values, reverse=True):
            yield payment_type, count

if __name__ == '__main__':
    PaymentTypesCount.run()
