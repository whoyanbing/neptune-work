from libs import utils
import json,sys

with open('config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

remote_conn =  utils.graph_connect(config['host'])

g = utils.graph_traversal(remote_conn)

if len(sys.argv) == 1:
    print('please input your operation [python3 gremlin.py show or python3 gremlin.py del]')
elif len(sys.argv) == 2 and sys.argv[1] == 'show':
    print("account vertex number:", g.V().hasLabel('account').count().toList())
    print("product vertex number:", g.V().hasLabel('product').count().toList())
    print("order edge number:", g.E().hasLabel('order').count().toList())
    print("reference edge number:", g.E().hasLabel('reference').count().toList())
elif len(sys.argv) == 2 and sys.argv[1] == 'del':
    print('please input which data you want to delete.\n[accout, product, order, or reference]')
elif len(sys.argv) == 2 and sys.argv[1] != 'show':
    print('input error.[python3 gremlin.py show or python3 gremlin.py del]')
elif len(sys.argv) == 3 and sys.argv[1] != 'del':
    print('input error.[python3 gremlin.py show or python3 gremlin.py del]')
elif len(sys.argv) == 3 and sys.argv[1] == 'del':
    del_obj = sys.argv[2]
    vertex_list = ['account', 'product']
    edge_list = ['order', 'reference']
    if del_obj not in vertex_list and del_obj not in edge_list:
        print("input error.[" + del_obj + " data doesn't exists]")
    else:
        user_choice = input("are you confirm to drop " + del_obj + " data? [y/n]")
        if user_choice == 'y' and del_obj in vertex_list:
                g.V().hasLabel(del_obj).drop().toList()
                print('drop ' + del_obj + ' vertex data completely.')
        elif user_choice == 'y' and del_obj in edge_list:
                g.E().hasLabel(del_obj).drop().toList()
                print('drop ' + del_obj + ' edge data completely.')
        elif user_choice == 'n':
            print('drop canceled!')
        else:
            print('input error.[y/n]')

remote_conn.close()
