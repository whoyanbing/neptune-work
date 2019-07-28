from main import graph_connect
from main import graph_traversal

remote_server = 'ws://localhost'
#remote_server = 'wss://recengineonpremdatasource.comltq8nzp9d.us-west-2.neptune.amazonaws.com'
remote_conn = graph_connect(remote_server)
g = graph_traversal(remote_conn)

print(g.V().hasLabel('account').count().toList)
print(g.V().hasLabel('product').count().toList)
print(g.E().hasLabel('order').count().toList)
print(g.E().hasLabel('reference').count().toList)

remote_conn.close()