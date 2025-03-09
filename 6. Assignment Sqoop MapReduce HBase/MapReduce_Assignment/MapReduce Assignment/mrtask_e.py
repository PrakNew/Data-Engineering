from mrjob.job import MRJob

class RevenueRatio(MRJob):

    def mapper(self, _, line):
        
        if not line.startswith('VendorID'):
            try:
                fields = line.split(',')
                pickup_location = fields[7]
                total_revenue = float(fields[16])
                tips = float(fields[13])

                
                yield pickup_location, (tips, total_revenue)
            except (ValueError, IndexError):
                pass  

    def combiner(self, pickup_location, tips_revenues):
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        yield pickup_location, (total_tips, total_revenue)

    def reducer(self, pickup_location, tips_revenues):
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        
        if total_revenue > 0:
            average_tips_to_revenue_ratio = total_tips / total_revenue
        else:
            average_tips_to_revenue_ratio = 0  
        
        yield pickup_location, average_tips_to_revenue_ratio


if __name__ == '__main__':
    RevenueRatio.run()

