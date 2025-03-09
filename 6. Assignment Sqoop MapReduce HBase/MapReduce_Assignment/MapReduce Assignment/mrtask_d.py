from mrjob.job import MRJob
from mrjob.step import MRStep

class AverageTripTime(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort_reducer)
        ]

    def mapper(self, _, line):
        # Skip the header row
        if not line.startswith('VendorID'):
            try:
                # Extract pickup location and trip time
                data = line.strip().split(',')
                pickup_location = data[7].strip()
                trip_time = float(data[4].strip())  # Assuming column 4 is trip time
                yield pickup_location, (trip_time, 1)
            except (IndexError, ValueError):
                pass  # Skip malformed lines

    def reducer(self, key, values):
        # Aggregate total trip time and count
        total_trip_time = 0
        total_count = 0
        for trip_time, count in values:
            total_trip_time += trip_time
            total_count += count

        if total_count > 0:
            # Calculate average trip time
            average_trip_time = total_trip_time / total_count
            yield None, (average_trip_time, key)

    def sort_reducer(self, _, values):
        # Sort pickup locations by average trip time in descending order
        for average_trip_time, pickup_location in sorted(values, reverse=True):
            yield pickup_location, average_trip_time

if __name__ == '__main__':
    AverageTripTime.run()

