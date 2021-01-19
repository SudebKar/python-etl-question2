import yaml
import pandas as pd

from utils import SALES_TABLE, REGION_TABLE, CUSTOMER_TABLE, DATA_PATH, DUMP_COLUMNS, SOURCE_FILE
from utils.metadata_util import Region, Customer, Sales, Base, metadata
from sqlalchemy import extract, Table, select


def parse_config(configfilepath):
    with open(configfilepath) as configYml:
        try:
            conf = yaml.load(configYml, Loader=yaml.FullLoader)
        except yaml.YAMLError as exc:
            raise exc
    return conf


# All the getters defined below to pull historical data from Sales and corresponding related tables
def get_sales_data(con, months):
    sales_tbl = Table(SALES_TABLE, metadata, autoload_with=con)
    result = select([sales_tbl]).where(extract('month', sales_tbl.columns.sales_date).in_(months))
    return pd.read_sql(result, con)


def get_cust_and_regions(sales_df):
    reg = [int(i) for i in sales_df["region_id"].unique()]
    cust = [int(i) for i in sales_df["customer_id"].unique()]
    return reg, cust


def get_region_data(con, regions):
    regions_tbl = Table(REGION_TABLE, metadata, autoload_with=con)
    result = select([regions_tbl]).where(regions_tbl.columns.region_id.in_(regions))
    return pd.read_sql(result, con)


def get_customer_data(con, customers):
    cust_tbl = Table(CUSTOMER_TABLE, metadata, autoload_with=con)
    result = select([cust_tbl]).where(cust_tbl.columns.customer_id.in_(customers))
    return pd.read_sql(result, con)

def write_data_to_csv(sales_df,rgns_df,cust_df):
    sales_df.to_csv(DATA_PATH + 'Sales.csv', index=False)
    rgns_df.to_csv(DATA_PATH + 'Regions.csv', index=False)
    cust_df.to_csv(DATA_PATH + 'Customers.csv', index=False)

def create_merged_sales_dump(sales_df,rgns_df,cust_df):
    merged_df = sales_df.merge(rgns_df, how="inner", on="region_id")
    merged_df = merged_df.merge(cust_df, how="inner", on="customer_id")

    result = merged_df[DUMP_COLUMNS]
    result.to_csv(DATA_PATH + 'SALES_FINAL.csv', index=False)

def read_source_flat_sales_data(file):
    source_df = pd.read_csv(file)
    return source_df

def load_sales_data(session,engine,src_df):
    """create the tables if the tables are not present and sets up the relationship between them
        Finally iterates through the input rows and inserts the same into the data model"""

    Customer.__table__.create(bind=engine, checkfirst=True)
    Region.__table__.create(bind=engine, checkfirst=True)
    Sales.__table__.create(bind=engine, checkfirst=True)

    try:
        for i,r in src_df.iterrows():
            region_rec = Region(r["region_name"], r["country"])
            customer_rec = Customer(r["customer_name"], r["customer_phone"], r["customer_address"])

            sales_rec = Sales(r["sales_amount"], r["sales_date"], customer_rec, region_rec)

            session.add(sales_rec)
        session.commit()
        success = 1
    except :
        session.rollback()
        success = 0
    finally :
        session.close()
        return success
