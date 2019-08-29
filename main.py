import json, os, logging
from libs import utils

def main():

    with open('config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)

    logging.basicConfig(filename='log/load.log', level=logging.DEBUG, format=config['log_format'], datefmt=config['date_format'])

    remote_conn =  utils.graph_connect(config['remote_host'])

    g_traversal = utils.graph_traversal(remote_conn)

    # purchase_history_files = os.listdir(config['purchase_history_path'])

    # try:
    #     for ph_file in purchase_history_files:
    #         utils.load_purchase_history(config['purchase_history_path'] + '/' + ph_file, g_traversal)
    # except Exception as e:
    #     logging.error(e)
    # else:
    #     logging.debug("load purchase_hisroty succefully!")

    manual_reference_files = os.listdir(config['manual_reference_path'])

    try:
        for mr_file in manual_reference_files:
            utils.load_manual_reference(config['manual_reference_path'] + '/' + mr_file, g_traversal)
    except Exception as e:
        logging.error(e)
    else:
        logging.debug("load manual_reference succefully!")

    remote_conn.close()

if __name__ == '__main__':
    main()
