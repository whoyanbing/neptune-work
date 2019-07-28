from __future__  import print_function
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
#from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import pandas as pd
import os

def graph_connect(server):
    return DriverRemoteConnection( server + ':8182/gremlin','g')

def graph_traversal(connect):
    graph = Graph()
    return graph.traversal().withRemote(connect)

def file_path_list(path, filename):
    file_list = os.listdir(path)
    file_path = []
    for file in file_list:
        file_account_by_cont = file + '/' + filename
        file_path.append(path + '/' + file_account_by_cont)
    return file_path

def nan_to_string(data):
	if type(data) == float:
		return ''
	return data

def load_purchase_history(filepath, graph_traversal):
    print('start load ' + filepath + '...')
    data_frame = pd.read_csv(filepath, sep='|', header=0, dtype=str)
    g = graph_traversal
    for index, row in data_frame.iterrows():
        if not g.V().has('objId', nan_to_string(row['Account ID'])).toList():
            g.addV('account').property('objId', nan_to_string(row['Account ID'])).\
            property('accountName1', nan_to_string(row['Account Name1'])).\
            property('accountName2', nan_to_string(row['Account Name2'])).\
            property('CRMContactID', nan_to_string(row['CRM Contact ID'])).\
            property('ECCContactID', nan_to_string(row['ECC Contact ID'])).\
            property('emailAddress', nan_to_string(row['Email Address'])).next()
        if not g.V().has('objId', nan_to_string(row['Material Master Product ID'])).toList():
            g.addV('product').property('objId', nan_to_string(row['Material Master Product ID'])).\
            property('productLine', nan_to_string(row['Product Line'])).\
            property('productItem', nan_to_string(row['Product Item'])).\
            property('productDescription', nan_to_string(row['Product Description'])).next()
        g.addE('order').from_(g.V().has('objId',nan_to_string(row['Account ID']))).\
            to(g.V().has('objId',nan_to_string(row['Material Master Product ID']))).\
            property('app', 'Rec_Engine').\
            property('orderNumber',nan_to_string(row['Order Number'])).\
            property('orderDate',nan_to_string(row['Order Date'])).\
            property('primaryProduct',nan_to_string(row['Primary Product'])).\
            property('purchaseOrder',nan_to_string(row['Purchase Order'])).\
            property('P.O.Date',nan_to_string(row['P.O. Date'])).\
            property('purchaseAmount',nan_to_string(row['Purchase/Net Amount'])).\
            property('targetQuantity',nan_to_string(row['Target Quantity'])).\
            property('orderQuantity',nan_to_string(row['Order Quantity'])).\
            property('orderType',nan_to_string(row['Order Type'])).\
            property('totalOrderValue',nan_to_string(row['Total Order Value'])).\
            property('soldToCountry',nan_to_string(row['Sold to Country'])).\
            property('marketCode', nan_to_string(row['Market Code'])).iterate()
    print('load ' + filepath + 'succefully!')

def load_product_reference(filepath, graph_traversal):
    print('start load ' + filepath + '...')
    dataframe = pd.read_csv(filepath, header=0, dtype='str')
    dataframe = dataframe.drop_duplicates(['PART_ID'], keep='first')
    dataframe = dataframe[dataframe['TYPE']=='CrossSellReference']
    g = graph_traversal
    for index, row in dataframe.iterrows():
	    if g.V().has('objId',nan_to_string(row['PART_ID'])).toList() and \
        g.V().has('objId',nan_to_string(row['REF_PART_ID'])).toList():
		    g.addE('reference').from_(g.V().has('objId',nan_to_string(row['PART_ID']))).\
            to(g.V().has('objId',nan_to_string(row['REF_PART_ID']))).\
            property('app', 'Rec_Engine').\
            property('Type',nan_to_string(row['TYPE'])).\
            property('metaValues',nan_to_string(row['META_VALUES'])).iterate()
    print('load ' + filepath + 'succefully!')

def main():
    print('load start!')
    remote_server = 'ws://localhost'
    #remote_server = 'wss://recengineonpremdatasource.comltq8nzp9d.us-west-2.neptune.amazonaws.com'
    remote_conn = graph_connect(remote_server)
    g_traversal = graph_traversal(remote_conn)
    file_list = file_path_list('data/purchase_history', 'PurchHist_by_cont.csv')
    for file in file_list:
        load_purchase_history(file, g_traversal)
    reference_data = 'data/manual_reference/PIM_ATG_PART_AND_PART_CROSSREFERENCE_201907101523.csv'
    load_product_reference(reference_data, g_traversal)
    remote_conn.close()
    print('load completed!')

if __name__ == "__main__":
    main()