from mrjob.job import MRJob
from mrjob.step import MRStep

TARGET_USERS = {'0012', '0141', '0009', '0276', '0507'}

class CoffeeSalesByUserMonth(MRJob):

    def mapper_extract_sales(self, _, line):
        parts = line.strip().split(',')
        if len(parts) != 7 or parts[1] not in TARGET_USERS:
            return
        try:
            user = parts[1]
            amount = float(parts[2])
            month = int(parts[4])
            year = int(parts[5])
            yield (user, year, month), amount
        except ValueError:
            pass

    def reducer_sum_by_month(self, user_date_key, amounts):
        total = sum(amounts)
        user, year, month = user_date_key
        yield user, (year, month, total)

    def reducer_sort_by_date(self, user, year_month_totals):
        sorted_records = sorted(year_month_totals, key=lambda x: (x[0], x[1]))
        for year, month, total in sorted_records:
            yield f'{user}: {year}-{month:02d}', round(total, 2)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_sales,
                   reducer=self.reducer_sum_by_month),
            MRStep(reducer=self.reducer_sort_by_date)
        ]

if __name__ == '__main__':
    CoffeeSalesByUserMonth.run()
