from __future__  import print_function  # Python 2/3 compatibility
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import csv
graph = Graph()
remoteConn = DriverRemoteConnection('ws://localhost:8182/gremlin','g')
g = graph.traversal().withRemote(remoteConn)
with open("input/PurchHist_by_acct[1][1].csv",'r',encoding="utf-8") as f:
	reader = csv.reader(f)
	fieldnames = next(reader)#获取数据的第一列，作为后续要转为字典的键名 生成器，next方法获取
	print(fieldnames)
	csv_reader = csv.DictReader(f,fieldnames=fieldnames) #self._fieldnames = fieldnames # list of keys for the dict 以list的形式存放键名
	for row in csv_reader:
		d={}
		for k,v in row.items():
			d[k]=v
	print(d)
with open('input/PurchHist_by_acct[1][1].csv')as f:
	next(f)
	for line in f:
		column = line.split('|')
		break
		if not g.V().has('AccountId', column[2]).toList():
			g.addV('account').property('AccountId', column[2]).\
            property('CompanyName1', column[0]).\
            property('CompanyName2', column[1]).\
            property('AccountRole', column[3]).next()
		if not g.V().has('MaterialMasterProductId', column[8]).toList():
			g.addV('product').property('ProductLine', column[6]).\
        	property('MaterialMasterProductId', column[8]).\
        	property('ProductDescription', column[9]).next()
		g.addE('order').from_(g.V().has('AccountId',column[2])).\
            to(g.V().has('MaterialMasterProductId',column[8])).\
            property('OrderNumber',column[4]).\
            property('OrderDate',column[5]).\
            property('Purchase/NetAmount',column[13]).\
            property('OrderQuantity',column[15]).\
            property('OrderType',column[16]).\
            property('SoldToCountry',column[17]).iterate()
file_purch_hisroty = open('input/PurchHist_by_cont[1][1].csv')
next(file_purch_hisroty)
for line in file_purch_hisroty:
    column = line.split('|')
    break
    g.addE('order').from_(g.V().has('AccountId',column[2])).\
            to(g.V().has('MaterialMasterProductId',column[8])).\
            property('OrderNumber',column[4]).\
            property('OrderDate',column[5]).\
            property('Purchase/NetAmount',column[13]).\
            property('OrderQuantity',column[15]).\
            property('OrderType',column[16]).\
            property('SoldToCountry',column[17]).iterate()
            #property('MarketCode',column[19].strip()).iterate()
file_purch_hisroty.close()
# print(g.V().count().toList())
# print(g.E().count().toList())
# print(g.V().hasLabel('account').limit(1).valueMap().toList())
# print(g.V().hasLabel('product').limit(1).valueMap().toList())
# print(g.E().hasLabel('order').limit(1).valueMap().toList())
remoteConn.close()
