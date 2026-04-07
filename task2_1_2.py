from mrjob.job import MRJob
from mrjob.step import MRStep

class CoffeeMonthlySales(MRJob):

    def mapper(self, _, line):
        parts = line.strip().split(',')
        if len(parts) != 7:
            return
        coffee = parts[0].strip()
        try:
            sale_amount = float(parts[2].strip())
            yr = int(parts[5].strip())
            mo = int(parts[4].strip())
        except ValueError:
            return

        target_coffees = {'Espresso', 'Cocoa', 'Cortado'}
        if coffee in target_coffees:
            yield (coffee, yr, mo), sale_amount

    def reducer_sum_by_month(self, key, amounts):
        coffee, yr, mo = key
        total = sum(amounts)
        yield coffee, (yr, mo, total)

    def reducer_sort_and_format(self, coffee, records):
        sorted_records = sorted(records, key=lambda date: date[:2])
        for yr, mo, total in sorted_records:
            date_str = f"{yr}-{mo:02d}"
            yield f"{coffee}: {date_str}", round(total, 2)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_sum_by_month),
            MRStep(reducer=self.reducer_sort_and_format)
        ]

if __name__ == '__main__':
    CoffeeMonthlySales.run()
