import pandas as pd

file_path = 'data/manual_reference/PIM_ATG_PART_AND_PART_CROSSREFERENCE_201907231521.csv'
df = pd.read_csv(file_path, header=0, dtype='str')
df = df[df['TYPE']=='CrossSellReference']
print(df.head())
print(len(df))