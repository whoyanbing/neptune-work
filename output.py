from pyignite import Client

client = Client()
client.connect('127.0.0.1', 10800)

OUTPUT_QUERY = '''SELECT * FROM City;OUTPUT TO 'test.csv'
'''

client.sql(OUTPUT_QUERY)