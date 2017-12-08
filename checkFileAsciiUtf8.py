#checkFileAsciiUtf8.py
#python #ascii #utf8 --python --ascii --utf8 
f = open("ug.txt","r")

for myLine in f:
    try:
      x = myLine.decode('ascii')
    except Exception as e:
        errorStr = 'ASCII ERROR: ' + str(e)
        print(errorStr)
        print(myLine)
    try:
      y = myLine.decode('utf-8')
    except Exception as e:
        errorStr = 'UTF-8 ERROR: ' + str(e)
        print(errorStr)
        print(myLine)

print("done")
