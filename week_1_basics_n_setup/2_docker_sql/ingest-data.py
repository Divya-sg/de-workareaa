
import pandas as pd
from sqlalchemy import create_engine
from time import time

import argparse
import os




def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    tblename = params.tblename
    url = params.url

    # download csv file
    csvfile = "yellow_taxi_data.csv"
    os.system(f"wget {url} -o {csvfile}")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    

    df_iter = pd.read_csv(csvfile, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=tblename, con=engine, if_exists='replace')
    df.to_sql(name=tblename, con=engine, if_exists='append')

    while True: 
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=tblename, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))


#df_zones = pd.read_csv('taxi+_zone_lookup.csv')

#df_zones.to_sql(name='zones', con=engine, if_exists='replace')



if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='Ingest Data to postgres')
    parser.add_argument('user', help='user for postgres')
    parser.add_argument('password', help='pwd for postgres')
    parser.add_argument('host', help='host for postges')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('db', help='DB for postgres')
    parser.add_argument('tblename', help='Table Name for data')
    parser.add_argument('url', help='url for csv')

    
    args = parser.parse_args()
    
    main(args)