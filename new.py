from __future__  import print_function  # Python 2/3 compatibility
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
graph = Graph()
remoteConn = DriverRemoteConnection('ws://localhost:8182/gremlin','g')
g = graph.traversal().withRemote(remoteConn)
file_purch_hisroty = open('input/PurchHist_by_cont[1][1].csv')
next(file_purch_hisroty)
for line in file_purch_hisroty:
    column = line.split('|')
    if not g.V().has('AccountId', column[2]).toList():
        g.addV('account').property('AccountId', column[2]).\
            property('AccountName1', column[0]).\
            property('AccountName2', column[1]).\
            property('EmailAddress', column[5]).next()
    if not g.V().has('MaterialMasterProductId', column[10]).toList():
        g.addV('product').property('ProductLine', column[8]).\
        property('MaterialMasterProductId', column[10]).\
        property('ProductDescription', column[11]).next()
file_purch_hisroty.close()
file_purch_hisroty = open('input/PurchHist_by_cont[1][1].csv')
next(file_purch_hisroty)
for line in file_purch_hisroty:
    column = line.split('|')
    g.addE('order').from_(g.V().has('AccountId',column[2])).\
            to(g.V().has('MaterialMasterProductId',column[10])).\
            property('OrderNumber',column[6]).\
            property('OrderDate',column[7]).\
            property('Purchase/NetAmount',column[15]).\
            property('OrderQuantity',column[17]).\
            property('OrderType',column[18]).\
            property('SoldToCountry',column[21]).\
            property('MarketCode',column[22].strip("\n")).iterate()
file_purch_hisroty.close()
print(g.V().count().toList())
print(g.E().count().toList())
print(g.V().hasLabel('account').limit(1).valueMap().toList())
print(g.V().hasLabel('product').limit(1).valueMap().toList())
print(g.E().hasLabel('order').limit(1).valueMap().toList())
remoteConn.close()
