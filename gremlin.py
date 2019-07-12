from __future__  import print_function  # Python 2/3 compatibility
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

graph = Graph()

remoteConn = DriverRemoteConnection('ws://localhost:8182/gremlin','g')
g = graph.traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin','g'))
file_part_ref = open('input/PIM_ATG_PART_AND_PART_CROSSREFERENCE_201907101523.csv')
file_purch_hisroty = open('input/PurchHist_by_cont[1][1].csv')
next(file_purch_hisroty)
for line in file_purch_hisroty:
    column = line.split('|')
    #print(column[0].strip("\""))
    #print(column[1])
    # if not g.V().has('AccountId', column[2]).toList():
    g.addV('account').property('AccountId', column[2]).next()
    # if not g.V().has('MaterialMasterProductId', column[10]).toList():
    g.addV('product').property('ProductLine', column[8]).\
        property('MaterialMasterProductId', column[10]).\
        property('ProductDescription', column[11]).next()
    # if not g.E().has('OrderNumber', column[6]).toList():
    g.addE('order').from_(g.V().has('AccountId',column[2])).\
            to(g.V().has('MaterialMasterProductId',column[10])).\
            property('OrderNumber',column[6]).\
            property('OrderDate',column[7]).\
            property('Purchase/NetAmount',column[15]).\
            property('OrderQuantity',column[16]).\
            property('OrderType',column[17]).\
            property('SoldToCountry',column[20]).\
            property('MarketCode',column[21]).\
                iterate()
    
    #break
# for line in file_part_ref:
#     column = line.split(',')
#     print(column)
#     break
#     one = g.V().has('product_id', column[0].strip("\"")).toList()
#     two = g.V().has('product_id', column[1]).toList()
#     if one and two:
#         g.addE('reference').from_(g.V().has('product_id', column[0].strip("\""))).to(g.V().has('product_id', column[1])).iterate()

print(g.V().count().toList())
print(g.E().count().toList())
print(g.V().hasLabel('account').limit(1).valueMap().toList())
print(g.V().hasLabel('product').limit(1).valueMap().toList())
print(g.E().hasLabel('order').limit(1).valueMap().toList())
remoteConn.close()