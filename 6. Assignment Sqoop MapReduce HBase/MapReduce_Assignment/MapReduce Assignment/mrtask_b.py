from mrjob.job import MRJob
from mrjob.step import MRStep

class PickupRevenueAnalysis(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.map_pickup_revenue, reducer=self.combine_revenue),
            MRStep(reducer=self.identify_top_pickup)
        ]
    
    def map_pickup_revenue(self, _, line):
        if not line.startswith('VendorID'):  # Skip the header row
            fields = line.strip().split(',')
            pickup_zone = fields[7]  # Pickup location is at index 7
            try:
                revenue_generated = float(fields[16])  # Total revenue is at index 16
                yield pickup_zone, revenue_generated
            except ValueError:
                pass  # Handle invalid or missing revenue values gracefully
    
    def combine_revenue(self, pickup_zone, revenues):
        total_revenue = sum(revenues)
        yield None, (total_revenue, pickup_zone)
    
    def identify_top_pickup(self, _, revenue_data):
        max_revenue, top_location = max(revenue_data)
        yield top_location, max_revenue


if __name__ == '__main__':
    PickupRevenueAnalysis.run()
