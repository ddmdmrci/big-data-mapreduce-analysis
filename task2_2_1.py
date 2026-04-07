from mrjob.job import MRJob
from mrjob.step import MRStep

class TopCustomersByCoffeeSales(MRJob):

    def mapper(self, _, line):
        
        parts = line.strip().split(',')
        if parts[0] == "coffee_type":
            return
        user_id = parts[1].strip()
        sales_amount = float(parts[2].strip())
        yield user_id, sales_amount

    def reducer_sum_sales(self, user_id, sales_amounts):
        
        yield None, (user_id, sum(sales_amounts))

    def reducer_find_top5(self, _, user_sales_pairs):
        
        top5_users = sorted(user_sales_pairs, key=lambda x: x[1], reverse=True)[:5]
        for user_id, total_sales in top5_users:
            yield user_id, round(total_sales, 2)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_sum_sales),
            MRStep(reducer=self.reducer_find_top5)
        ]

if __name__ == '__main__':
    TopCustomersByCoffeeSales.run()
