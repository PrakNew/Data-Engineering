# Task A: Identify the vendor with the highest total revenue
from mrjob.job import MRJob
from mrjob.step import MRStep

class VendorRevenueAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.map_revenue, reducer=self.aggregate_revenue),
            MRStep(reducer=self.find_max_revenue)
        ]
    
    def map_revenue(self, _, line):
        if not line.startswith('VendorID'):  # Skip the header row
            details = line.strip().split(',')
            vendor = details[0]  # VendorID is at index 0
            try:
                trip_revenue = float(details[16])  # Total revenue is at index 16
                yield vendor, trip_revenue
            except ValueError:
                pass  # Skip invalid or missing revenue values
    
    def aggregate_revenue(self, vendor, revenues):
        total_earnings = sum(revenues)
        yield None, (total_earnings, vendor)
    
    def find_max_revenue(self, _, aggregated_data):
        highest_revenue, top_vendor = max(aggregated_data)
        yield top_vendor, highest_revenue


if __name__ == '__main__':
    VendorRevenueAnalysis.run()
