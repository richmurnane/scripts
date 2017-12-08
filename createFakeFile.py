#createFakeFile.py

import os
import csv
import pandas as pd
from faker import Factory

fake = Factory.create()

myNumRecords = os.getenv('NUM_RECORDS', "50000")
myLineNum = 0

myList = []
for x in range(int(myNumRecords)):
    myLineNum = myLineNum + 1
    fakeRandomNum = fake.random_number()
    fakeNum2 = fakeRandomNum - 2
    myList.append([myLineNum, fake.first_name(), fake.last_name(), fake.street_address(), fake.city(), fake.state(),
        fake.postalcode(), fake.company(), fake.job(), fake.safe_email(), fake.uuid4(), fakeRandomNum, fakeNum2] )

myLabels = ["lineNum","firstName","lastName","streetAddress","city","stateProv","postal","company","job","email","guid","randomNumber","otherNumber"]

df = pd.DataFrame.from_records(myList, columns=myLabels)
df.to_csv("fake-data05.csv", sep=',', encoding='utf-8', index=False, quoting=csv.QUOTE_NONNUMERIC)
