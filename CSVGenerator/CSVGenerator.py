import random
import csv

with open('generatedCSV.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    for i in range(40000):
        print("CSV row created")
        writer.writerow([random.randint(0,3245),random.randint(0,24352435),random.randint(0,42352435),random.randint(0,24352345),random.randint(0,23452345),random.randint(0,23452345),random.randint(0,234523452435)])