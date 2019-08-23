#replace to your python path
!#/usr/bin/python3

#import pyignite package
from pyignite import Client

#set username and password
client = Client(username='', password='')

#set host and port
client.connect('127.0.0.1', 10800)

#replace 'CITY' to your table name
DATA_QUERY = '''SELECT * FROM CITY'''

results = client.sql(DATA_QUERY)

#set export file name
output_filename = 'manual_reference.csv'

with open(output_filename, 'w', encoding='utf-8') as f:
    for row in results:
        row = list(map(str, row))
        row = ','.join(row)
        f.write(row)
        f.write('\n')