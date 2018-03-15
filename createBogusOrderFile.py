# createBogusOrderFile.py

import os
import json
import pandas as pd
from faker import Factory
import datetime
import random

fake = Factory.create()

myNumRecords = os.getenv('NUM_RECORDS', "25000")
myOrderId = 0

d = []

for x in range(int(myNumRecords)):
    myOrderId = myOrderId + 1
    myCustomerId = random.randrange(2,99990)
    myDate = fake.past_date(start_date="-1500d", tzinfo=None)
    myMonth = "00" + str(myDate.month)
    myDay = "00" + str(myDate.day)
    myDateStr = str(myDate.year) + myMonth[-2:] + myDay[-2:]
    myOrderAmount = random.randrange(0,9999)

    d.append({
        '01-OrderId': str(myOrderId),
        '02-CustomerId': str(myCustomerId),
        '03-OrderDate': str(myDateStr),
        '04-OrderAmount': str(myOrderAmount),
        '05-Origin': "faker.factory"})

df = pd.DataFrame(d)

file_name = "~/fake_orders.txt"
df.to_csv(file_name, sep='|', encoding='utf8',index=False)

print "end" + str(len(df.index))
