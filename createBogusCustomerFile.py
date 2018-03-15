# createBogusFile.py
import os
import json
import pandas as pd
from faker import Factory

fake = Factory.create()

myNumRecords = os.getenv('NUM_RECORDS', "100000")
myLineNum = 0

d = []

for x in range(int(myNumRecords)):
    myLineNum = myLineNum + 1
    myFname = fake.first_name()
    myLname = fake.last_name()
    myStreetAddress = fake.street_address()
    myCity = fake.city()
    myStateProv = fake.state()
    myPostal = fake.postalcode()
    myCompany = fake.company()
    myJob = fake.job()
    myEmail = fake.safe_email()
    myGuid = fake.uuid4()
    myPwd = fake.password()
    myRandomNumber = fake.random_number()
    myIpv4 = fake.ipv4(network=False)
    myPhone = fake.phone_number()
    myDobJsonString = str(fake.profile(fields="birthdate"))
    myDobJsonString = myDobJsonString.replace("\'", "\"")
    myDobParsedJson = json.loads(myDobJsonString)
    myDob = str(myDobParsedJson['birthdate'])

    d.append({
        '01-lineNum': str(myLineNum),
        '02-Fname': str(myFname),
        '03-Lname': str(myLname),
        '04-StreetAddress': str(myStreetAddress),
        '05-City': str(myCity),
        '06-StateProv': str(myStateProv),
        '07-Postal': str(myPostal),
        '08-Company': str(myCompany),
        '09-Job': str(myJob),
        '10-email': str(myEmail),
        '11-RandomNumber': str(myRandomNumber),
        '12-guid': str(myGuid),
        '13-ipv4': str(myIpv4),
        '14-phone': str(myPhone),
        '15-dob': str(myDob),
        '16-Origin': "faker.factory"})

df = pd.DataFrame(d)

file_name = "~/fake_customers.txt"
df.to_csv(file_name, sep='|', encoding='utf8',index=False)

print "end" + str(len(df.index))
