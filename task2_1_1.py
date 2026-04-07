from mrjob.job import MRJob
from mrjob.step import MRStep

class MonthlySalesPerCoffee(MRJob):

    def mapper(self, _, line):
        fields = line.strip().split(',')
        if fields[0] == "coffee_type":
            return
        coffee_type = fields[0].strip()
        amount = float(fields[2].strip())
        yield coffee_type, amount

    def reducer(self, coffee_type, amounts):
        yield None, (coffee_type, sum(amounts))

    def reducer_find_bottom3(self, _, coffee_totals):
        sorted_coffees = sorted(coffee_totals, key=lambda x: x[1])
        for coffee in sorted_coffees[:3]:  
            yield coffee[0], coffee[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_bottom3)
        ]

if __name__ == '__main__':
    MonthlySalesPerCoffee.run()
