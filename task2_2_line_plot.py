import matplotlib.pyplot as plt
from datetime import datetime
from collections import defaultdict

def plot_user_monthly_sales(input_path="task2_2_2_output.txt", output_path="task2_2_output.pdf"):
    user_sales = defaultdict(list)

    with open(input_path, "r") as file:
        for line in file:
            parts = line.strip().split('\t')
            if len(parts) != 2:
                continue
            key, value = parts
            if ": " not in key:
                continue
            user_id, date_str = key.replace('"', '').split(": ")
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m")
                amount = float(value)
            except ValueError:
                continue
            user_sales[user_id].append((date_obj, amount))

    plt.figure(figsize=(12, 6))

    target_users = ['0012', '0141', '0009', '0276', '0507']

    for user in target_users:
        if user in user_sales:
            data = sorted(user_sales[user])
            dates = [d[0] for d in data]
            totals = [d[1] for d in data]
            plt.plot(dates, totals, marker='o', label=f"User {user}")

    plt.title("Top Users' Monthly Coffee Spending")
    plt.xlabel("Date (Year-Month)")
    plt.ylabel("Total Spending")
    plt.xticks(rotation=45)
    plt.legend(title="User ID")
    plt.tight_layout()
    plt.savefig(output_path)

if __name__ == "__main__":
    plot_user_monthly_sales()
