import csv
from tabulate import tabulate

def print_csv_as_table(file_path):
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)
        if not data:
            print("The CSV file is empty.")
            return
        headers = data[0]
        headers.append("diff")
        rows = data[1:]
        for row in rows:
            row.append(int(row[1])-int(row[2]))
        print(tabulate(rows, headers=headers, tablefmt="grid"))  # Options: grid, fancy_grid, pipe, etc.

# Example usage
print_csv_as_table("temp.csv")
