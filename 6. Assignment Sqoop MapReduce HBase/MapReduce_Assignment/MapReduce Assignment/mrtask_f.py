from mrjob.job import MRJob
from datetime import datetime

class RevenueByTime(MRJob):

    def convert_datetime(self, datetime_str):
        date_formats = ['%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
        for fmt in date_formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                pass
        raise ValueError('Unable to parse the datetime string')

    def mapper(self, _, line):
        # Skip the header row
        if not line.startswith('VendorID'):
            data = line.split(',')
            revenue_value = float(data[16])
            timestamp = self.convert_datetime(data[1])
            month = timestamp.month
            hour = timestamp.hour
            weekday = timestamp.weekday()
            yield (month, hour, weekday), revenue_value

    def reducer(self, time_key, revenues):
        total_revenue = 0
        trip_count = 0

        for revenue in revenues:
            total_revenue += revenue
            trip_count += 1

        avg_revenue_per_trip = total_revenue / trip_count

        yield time_key, avg_revenue_per_trip

if __name__ == '__main__':
    RevenueByTime.run()

