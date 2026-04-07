from pymongo import MongoClient
from datetime import datetime

class CoffeeDataProcessor:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["COMP6210-Assignment1"]
        self.source_collection = self.db["coffee_sales"]
        self.target_collection = self.db["coffee_extracted"]
        self.cleaned_data = []

    def extract_transactions(self):
        for entry in self.source_collection.find():
            card_id = entry.get("card")
            timestamp = entry.get("datetime")

            
            if not card_id or not timestamp:
                continue

            
            try:
                dt = timestamp if isinstance(timestamp, datetime) else datetime.strptime(timestamp.split(".")[0], "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

            record = (
                entry.get("coffee_name", "Unknown"),
                card_id[-4:],  
                float(entry.get("money", 0)),
                dt.day,
                dt.month,
                dt.year,
                dt.strftime("%H:%M:%S")
            )

            self.cleaned_data.append(record)

            self.target_collection.insert_one({
                "coffee_type": record[0],
                "user_id": record[1],
                "money": record[2],
                "day": record[3],
                "month": record[4],
                "year": record[5],
                "time": record[6]
            })

    def export_to_file(self, filename="task1_1_output.txt"):
        with open(filename, "w") as file:
            for row in self.cleaned_data:
                file.write(",".join(map(str, row)) + "\n")

    def process(self):
        self.extract_transactions()
        self.export_to_file()

if __name__ == "__main__":
    processor = CoffeeDataProcessor()
    processor.process()
