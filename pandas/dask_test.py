import dask.dataframe as dd
import dask
from dask.distributed import Client


def copy_columns(df, column_map):
    df[column_map['column']] = df[column_map['source']]
    return df


def read_csv(path):
    return dd.read_csv(path)


def write_to_csv(df):
    df.to_csv('movie_dask.csv')

if __name__ == '__main__':
    client = Client()
    column_map = {'column': 'director_usd', 'source': 'director_name'}
    dfs = client.map(read_csv, ['movie.csv'])
    dfs = client.map(copy_columns, dfs, [column_map])
    l = client.map(write_to_csv,dfs)
    client.submit(len, l).result()

    print("start")
