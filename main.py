import pandas as pd
import sys, os

def parse_csv(filename, columns, new_filename, sep='|'):
    #load csv file
    df = pd.read_csv(filename, header=0, sep=sep, encoding='utf-8')
    #choose useful columns
    df = df[columns]
    #write to a new csv file
    df.to_csv('tmp/' + new_filename + '.csv', index=False, encoding='utf-8')

def rename_header(filename, new_columns, label, name, index_label=None, index=False):
    #load csv file
    df = pd.read_csv('tmp/' + filename, header=0, encoding='utf-8')
    if(index):
        df.drop_duplicates(['PART_ID'], keep='first', inplace=True)
    #rename column name
    df.rename(columns=new_columns, inplace=True)
    #add label column
    df['~label'] = label
    df.to_csv('output/' + name + '_' + label + '.csv', index_label=index_label, index=index, encoding='utf-8')

def remove_tmp(path):
    file_list = os.listdir(path)
    for file in file_list:
            os.remove(path + '/' + file)

def main():
    #load input data file
    file_purch_history = sys.argv[1]

    file_manual_reference = sys.argv[2]

    account_columns = ['Account ID', 'Account Name1', 'Account Name2', 'CRM Contact ID', 'ECC Contact ID']
    
    product_columns = ['Material Master Product ID', 'Product Item', 'Product Line', 'Product Description']
    
    order_columns = ['Order Number', 'Account ID', 'Material Master Product ID']

    reference_columns = ['PART_ID', 'REF_PART_ID']
    
    parse_csv(file_purch_history, account_columns, 'account')
    
    parse_csv(file_purch_history, product_columns, 'product')
    
    parse_csv(file_purch_history, order_columns, 'order')

    parse_csv(file_manual_reference, reference_columns, 'reference', ',')

    #rename headers
    new_columns_account = {'Account ID':'~id', 'Account Name1':'account_name1:String', 'Account Name2':'account_name2:String', 'CRM Contact ID':'CRM_contact_id:String', 'ECC Contact ID':'ECC_contact_id:String'}
    
    new_columns_product = {'Material Master Product ID':'~id', 'Product Item':'product_item:String', 'Product Line':'product_line:String', 'Product Description':'product_desc:String'}
    
    new_columns_order = {'Order Number':'~id', 'Account ID':'~from', 'Material Master Product ID':'~to'}

    new_columns_reference = {'PART_ID':'~from', 'REF_PART_ID':'~to'}
    
    rename_header('account.csv', new_columns_account, 'account', 'vertex')

    rename_header('product.csv', new_columns_product, 'product', 'vertex')

    rename_header('order.csv', new_columns_order, 'order', 'edge')

    rename_header('reference.csv', new_columns_reference, 'reference', 'edge', '~id', True)

    #remove tmp file
    path = 'tmp'
    remove_tmp(path)

if __name__ == '__main__':
    #calling main function
    main()
