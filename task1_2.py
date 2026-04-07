from mrjob.job import MRJob
from mrjob.step import MRStep

class TotalCoffeeRevenue(MRJob):

    def map_coffee_sales(self, _, line):
        try:
            parts = line.strip().split(',')
            coffee = parts[0].strip()
            amount = float(parts[2].strip())
            yield coffee, amount
        except:
            
            return

    def combine_revenue(self, coffee, values):
        yield coffee, sum(values)

    def reduce_total_revenue(self, coffee, values):
        yield coffee, round(sum(values), 2)

    def steps(self):
        return [
            MRStep(
                mapper=self.map_coffee_sales,
                combiner=self.combine_revenue,
                reducer=self.reduce_total_revenue
            )
        ]

if __name__ == '__main__':
    TotalCoffeeRevenue.run()
