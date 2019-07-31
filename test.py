#import pandas as pd
import os
# file_path = 'data/manual_reference/PIM_ATG_PART_AND_PART_CROSSREFERENCE_201907231521.csv'
# df = pd.read_csv(file_path, header=0, dtype='str')
# df = df[df['TYPE']=='CrossSellReference']
# print(df.head())
# print(len(df))

def path_list(path):
    path_list = []
    dir_list = os.listdir(path)
    for directory in dir_list:
        file_list = os.listdir(path + '/' + directory)
        for file in file_list:
            path_list.append(path + '/' + directory + '/' + file)
    return path_list

res = file_list('data/purchase_history')
print(res)