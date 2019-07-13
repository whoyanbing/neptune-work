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
df = pd.read_csv('input/PIM_ATG_PART_AND_PART_CROSSREFERENCE_201907101523.csv', header=0, dtype='str')
df = df.drop_duplicates(['PART_ID'], keep='first')
for i, row in df.iterrows():
	if g.V().has('MaterialMasterProductId',nan_to_space(row['PART_ID'])).toList() and g.V().has('MaterialMasterProductId',nan_to_space(row['REF_PART_ID'])).toList():
		g.addE('reference').from_(g.V().has('MaterialMasterProductId',nan_to_space(row['PART_ID']))).\
            to(g.V().has('MaterialMasterProductId',nan_to_space(row['REF_PART_ID']))).\
            property('Type',nan_to_space(row['TYPE'])).property('MetaValues',nan_to_space(row['META_VALUES'])).iterate()
print(g.V().count().toList())
print(g.E().count().toList())
print(g.V().hasLabel('account').limit(1).valueMap().toList())
print(g.V().hasLabel('product').limit(1).valueMap().toList())
print(g.E().hasLabel('order').limit(1).valueMap().toList())
print(g.E().hasLabel('reference').limit(1).valueMap().toList())
remoteConn.close()