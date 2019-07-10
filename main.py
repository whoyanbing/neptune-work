import pandas as pd
import sys, os

def parse_csv(filename, columns, new_filename):
    #load csv file
    df = pd.read_csv(filename, header=0, sep='|', encoding='utf-8')
    #choose useful columns
    df = df[columns]
    #write to a new csv file
    df.to_csv('tmp/' + new_filename + '.csv', index=False, encoding='utf-8')

def rename_header(filename, new_columns, label):
    #load csv file
    df = pd.read_csv('tmp/' + filename, header=0, encoding='utf-8')
    #rename column name
    df.rename(columns=new_columns, inplace=True)
    #add label column
    df['~label'] = label
    df.to_csv('output/' + label + '.csv', index=False, encoding='utf-8')

def remove_tmp(path):
    file_list = os.listdir(path)
    for file in file_list:
            os.remove(path + '/' + file)

def main():
    #load input file and split to tree csv files
    filename = sys.argv[1]

    account_columns = ['Account ID', 'Account Name1', 'Account Name2', 'CRM Contact ID', 'ECC Contact ID']
    
    product_columns = ['Material Master Product ID', 'Product Item', 'Product Line', 'Product Description']
    
    order_columns = ['Order Number', 'Account ID', 'Material Master Product ID']
    
    parse_csv(filename, account_columns, 'account')
    
    parse_csv(filename, product_columns, 'product')
    
    parse_csv(filename, order_columns, 'order')

    #rename headers for the three file above
    new_columns_account = {'Account ID':'~id', 'Account Name1':'account_name1:String', 'Account Name2':'account_name2:String', 'CRM Contact ID':'CRM_contact_id:String', 'ECC Contact ID':'ECC_contact_id:String'}
    
    new_columns_product = {'Material Master Product ID':'~id', 'Product Item':'product_item:String', 'Product Line':'product_line:String', 'Product Description':'product_desc:String'}
    
    new_columns_order = {'Order Number':'~id', 'Account ID':'~from', 'Material Master Product ID':'~to'}
    
    rename_header('account.csv', new_columns_account, 'account')

    rename_header('product.csv', new_columns_product, 'product')

    rename_header('order.csv', new_columns_order, 'order')

    #remove tmp file
    path = 'tmp'
    remove_tmp(path)

if __name__ == '__main__':
    #calling main function
    main()
