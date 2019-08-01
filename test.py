from pyignite import Client

client = Client()
client.connect('127.0.0.1', 10800)

CITY_CREATE_TABLE_QUERY = '''CREATE TABLE City (
    ID INT(11),
    Name CHAR(35),
    CountryCode CHAR(3),
    District CHAR(20),
    Population INT(11),
    PRIMARY KEY (ID, CountryCode)
)'''

client.sql(CITY_CREATE_TABLE_QUERY)

CITY_INSERT_QUERY = '''INSERT INTO City(
    ID, Name, CountryCode, District, Population
) VALUES (?, ?, ?, ?, ?)'''

CITY_DATA = [
[1,'Kabul','AFG','Kabol',1780000],
[2,'Qandahar','AFG','Qandahar',237500],
[3,'Herat','AFG','Herat',186800],
[4,'Mazar-e-Sharif','AFG','Balkh',127800],
[5,'Amsterdam','NLD','Noord-Holland',731200]
]

for row in CITY_DATA:
    client.sql(CITY_INSERT_QUERY, query_args=row)