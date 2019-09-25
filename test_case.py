from libs import utils
import json, os

with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)

remote_conn = utils.graph_connect(config['host'])
    
g = utils.graph_traversal(remote_conn)

purchase_history_files = os.listdir(config['purchase_history_path'])

for ph_file in purchase_history_files:
        utils.purchase_history_test(config['purchase_history_path'] + '/' + ph_file, g)

manual_reference_files = os.listdir(config['manual_reference_path'])

for mr_file in manual_reference_files:
        utils.manual_reference_test(config['manual_reference_path'] + '/' + mr_file, g)

remote_conn.close()
