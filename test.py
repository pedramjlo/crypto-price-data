import pandas as pd



csv_data = './saved_data/saved_data.csv'


def read_file(data=csv_data):
    return pd.read_csv(data)





df = read_file()
print(df.dtypes)

