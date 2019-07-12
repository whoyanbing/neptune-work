from __future__  import print_function  # Python 2/3 compatibility
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import pandas as pd
graph = Graph()
remoteConn = DriverRemoteConnection('ws://localhost:8182/gremlin','g')
g = graph.traversal().withRemote(remoteConn)
df = pd.read_csv('input/PIM_ATG_PART_AND_PART_CROSSREFERENCE_201907101523.csv', header=0, dtype='str')
df = df.drop_duplicates(['PART_ID'], keep='first')
for i, row in df.iterrows():
	if g.V().has('MaterialMasterProductId',row['PART_ID']).toList() and g.V().has('MaterialMasterProductId',row['REF_PART_ID']).toList():
		g.addE('reference').from_(g.V().has('MaterialMasterProductId',row['PART_ID'])).\
            to(g.V().has('MaterialMasterProductId',row['REF_PART_ID'])).\
            property('Type',row['TYPE']).property('MetaValues',row['META_VALUES']).iterate()
print(g.V().count().toList())
print(g.E().count().toList())
print(g.V().hasLabel('account').limit(1).valueMap().toList())
print(g.V().hasLabel('product').limit(1).valueMap().toList())
print(g.E().hasLabel('order').limit(1).valueMap().toList())
print(g.E().hasLabel('reference').limit(1).valueMap().toList())
remoteConn.close()