from __future__  import print_function
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import pandas as pd
import sys

def gremlin_connect(server):
    graph = Graph()
    with DriverRemoteConnection(server + ':8182/gremlin','g') as remoteConn:
        return graph.traversal().withRemote(remoteConn)
        
def nan_to_string(row_data):
    if type(row_data) == float:
        return ''
    return row_data

def parse_data(filename, sep=','):
    data_frame = pd.read_csv(filename, sep=sep, header=0, dtype=str, encoding='utf-8')
    return data_frame

def load_purch_history(filename, graph_traversal, sep='|'):
    purch_data_frame = parse_data(filename, sep=sep)
    g = graph_traversal
    for index, row in purch_data_frame.iterrows():
        if not g.V().has('AccountId', row['Account ID']).toList():
            g.addV('account').property('AccountId', row['Account ID']).\
            property('AccountName1', row['Account Name1']).\
            property('AccountName2', nan_to_string(row['Account Name2'])).\
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
            property('MarketCode', nan_to_string(row['Market Code'])).\
            property('SoldToCountry',row['Sold to Country']).iterate()

def load_manu_reference(filename, graph_traversal, sep=','):
    refer_data_frame = parse_data(filename=filename, sep=sep)
    g = graph_traversal
