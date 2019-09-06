from libs import utils
import json

with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)

remote_conn =  utils.graph_connect(config['host'])

g_traversal = utils.graph_traversal(remote_conn)

manual_prd_refer = "data/Recommendation_Engine_PROD_LOAD_FILE.xlsx"

utils.load_manual_prod_refer(manual_prd_refer, g_traversal)
        
remote_conn.close()