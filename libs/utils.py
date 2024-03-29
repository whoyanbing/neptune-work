from __future__  import print_function
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import pandas as pd

def graph_connect(server):
    return DriverRemoteConnection(server + ':8182/gremlin','g')

def graph_traversal(connect):
    graph = Graph()
    return graph.traversal().withRemote(connect)

def nan_to_string(data):
	if type(data) == float:
		return ''
	return data

def load_purchase_history(filepath, graph_traversal):
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
        if not g.V().has('account', 'objId', nan_to_string(row['Account ID'])).out('order').has('product', 'objId', nan_to_string(row['Material Master Product ID'])).hasNext():
            g.addE('order').from_(g.V().has('objId', nan_to_string(row['Account ID']))).\
            to(g.V().has('objId', nan_to_string(row['Material Master Product ID']))).\
            property('app', 'Rec_Engine').\
            property('orderNumber', nan_to_string(row['Order Number'])).\
            property('orderDate', nan_to_string(row['Order Date'])).\
            property('primaryProduct', nan_to_string(row['Primary Product'])).\
            property('purchaseOrder', nan_to_string(row['Purchase Order'])).\
            property('P.O.Date', nan_to_string(row['P.O. Date'])).\
            property('purchaseAmount', nan_to_string(row['Purchase/Net Amount'])).\
            property('targetQuantity', nan_to_string(row['Target Quantity'])).\
            property('orderQuantity', nan_to_string(row['Order Quantity'])).\
            property('orderType', nan_to_string(row['Order Type'])).\
            property('totalOrderValue', nan_to_string(row['Total Order Value'])).\
            property('soldToCountry', nan_to_string(row['Sold to Country'])).\
            property('marketCode', nan_to_string(row['Market Code'])).iterate()

def load_manual_reference(filepath, graph_traversal):
    dataframe = pd.read_csv(filepath, header=0, dtype=str)
    #dataframe = dataframe.drop_duplicates(['PART_ID'], keep='first')
    dataframe = dataframe[dataframe['TYPE'].str.strip() == 'CrossSellReference']
    g = graph_traversal
    for index, row in dataframe.iterrows():
        if not g.V().has('objId', nan_to_string(row['PART_ID'])).toList():       
            g.addV('product').property('objId', nan_to_string(row['PART_ID'])).next()
        if not g.V().has('objId', nan_to_string(row['REF_PART_ID']).strip()).toList():
            g.addV('product').property('objId', nan_to_string(row['REF_PART_ID']).strip()).next()
        if not g.V().has('product', 'objId', nan_to_string(row['PART_ID'])).out('reference').has('product', 'objId', nan_to_string(row['REF_PART_ID']).strip()).hasNext():
            g.addE('reference').from_(g.V().has('objId',nan_to_string(row['PART_ID']))).\
            to(g.V().has('objId', nan_to_string(row['REF_PART_ID']).strip())).\
            property('app', 'Rec_Engine').iterate()

def purchase_history_test(filepath, graph_traversal):
    data_frame = pd.read_csv(filepath, sep='|', header=0, dtype=str)
    g = graph_traversal
    for index, row in data_frame.iterrows():
        if not g.V().has('account', 'objId', nan_to_string(row['Account ID'])).out('order').has('product', 'objId', nan_to_string(row['Material Master Product ID'])).hasNext():
            print(row['Account ID'])

def manual_reference_test(filepath, graph_traversal):
    dataframe = pd.read_csv(filepath, header=0, dtype=str)
    dataframe = dataframe[dataframe['TYPE'].str.strip() == 'CrossSellReference']
    g = graph_traversal
    for index, row in dataframe.iterrows():
        if not g.V().has('product', 'objId', nan_to_string(row['PART_ID'])).out('reference').has('product', 'objId', nan_to_string(row['REF_PART_ID']).strip()).hasNext():
            print(row['PART_ID'])
            
if __name__ == '__main__':
    main()
