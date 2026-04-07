import matplotlib.pyplot as plt
from datetime import datetime

def draw_sales_trend(input_file="task2_1_2_output.txt", output_pdf="task2_1_output.pdf"):
    coffee_dict = {}

    file = open(input_file, "r")
    lines = file.readlines()
    file.close()

    for line in lines:
        parts = line.strip().replace('"', '').split("\t")
        if len(parts) == 2 and ": " in parts[0]:
            coffee_type, month = parts[0].split(": ")
            sales_value = float(parts[1])
            if coffee_type not in coffee_dict:
                coffee_dict[coffee_type] = {}
            coffee_dict[coffee_type][month] = sales_value

    plt.figure(figsize=(12, 6))

    coffee_list = ["Espresso", "Cocoa", "Cortado"]

    for coffee in coffee_list:
        if coffee in coffee_dict:
            month_sales = coffee_dict[coffee]
            sorted_months = sorted(month_sales.keys(), key=lambda d: datetime.strptime(d, "%Y-%m"))
            x_points = [datetime.strptime(m, "%Y-%m") for m in sorted_months]
            y_points = [month_sales[m] for m in sorted_months]
            plt.plot(x_points, y_points, label=coffee, marker='o')

    plt.xlabel("Month-Year")
    plt.ylabel("Total Sales Amount")
    plt.title("Sales Trend of Bottom 3 Coffee Types")

    if coffee_dict:
        plt.legend(loc="upper left", title="Coffee Names")

    plt.xticks(rotation=45)
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.tight_layout()

    plt.savefig(output_pdf)
    plt.close()

if __name__ == "__main__":
    draw_sales_trend()
