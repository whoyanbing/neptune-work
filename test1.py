from __future__  import print_function  # Python 2/3 compatibility
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import pandas as pd
graph = Graph()
local_server = 'ws://localhost'
remote_server = 'wss://recengineonpremdatasource.comltq8nzp9d.us-west-2.neptune.amazonaws.com'
remoteConn = DriverRemoteConnection(local_server +':8182/gremlin','g')
g = graph.traversal().withRemote(remoteConn)
def nan_to_space(data):
	if type(data) == float:
		return ''
	return data
df = pd.read_csv('input/PurchHist_by_cont[1][1].csv', sep='|', header=0, dtype='str')
for i, row in df.iterrows():
    if not g.V().has('AccountId', row['Account ID']).toList():
        g.addV('account').property('AccountId', row['Account ID']).\
            property('AccountName1', row['Account Name1']).\
            property('AccountName2', nan_to_space(row['Account Name2'])).\
            property('EmailAddress', row['Email Address']).next()
    if not g.V().has('MaterialMasterProductId', row['Material Master Product ID']).toList():
        g.addV('product').property('ProductLine', row['Product Line']).\
        	property('MaterialMasterProductId', row['Material Master Product ID']).\
        	property('ProductDescription', row['Product Description']).next()
    g.addE('order').from_(g.V().has('AccountId',row['Account ID'])).\
            to(g.V().has('MaterialMasterProductId',row['Material Master Product ID'])).\
            property('OrderNumber',row['Order Number']).\
            property('OrderDate',row['Order Date']).\
            property('Purchase/NetAmount',row['Purchase/Net Amount']).\
            property('OrderQuantity',row['Order Quantity']).\
            property('OrderType',row['Order Type']).\
            property('MarketCode', nan_to_space(row['Market Code'])).\
            property('SoldToCountry',row['Sold to Country']).iterate()

print(g.V().count().toList())
print(g.E().count().toList())
print(g.V().hasLabel('account').limit(1).valueMap().toList())
print(g.V().hasLabel('product').limit(1).valueMap().toList())
print(g.E().hasLabel('order').limit(1).valueMap().toList())
remoteConn.close()