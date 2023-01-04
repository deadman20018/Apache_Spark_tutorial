import random
import csv

with open('generatedCSV.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    for i in range(10000):
        writer.writerow([random.randint(0,100), random.randint(0,200), random.randint(0,2000), random.randint(0,1000000)])