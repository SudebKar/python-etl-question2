from sqlalchemy.orm import sessionmaker
from utils.connections import DbConnect
from utils.custom_util import parse_config, get_region_data, get_customer_data, get_cust_and_regions, get_sales_data, \
    write_data_to_csv, create_merged_sales_dump, DATA_PATH, SOURCE_FILE, read_source_flat_sales_data, load_sales_data
import logging
import datetime


if __name__ == '__main__':
    """If 'pull' option is selected in the config file as mode of operation, 
                then data is picked up from database and csv dumps as well as consolidated/merged sql dump is created 
    If 'load' is selected in the config file as mode of operation,
                then data is picked up from a source sales csv file and loaded into the database 
                by maintaining the relationship created in sqlalchemy ORM"""

    logging.basicConfig(filename='log/sales_pull_log.log', filemode='w', level=logging.INFO)
    # getting the configs from yml file and creating db connection
    config = parse_config('./config/test_config.yml')
    conf = config.get('db.config')
    db = DbConnect(conf.get('driver'), conf.get('host'), conf.get('port')
                   , conf.get('username'), conf.get('password'), conf.get('database'))
    con = db.create_sql_engine()

    mode = config.get('mode')
    if mode == 'pull':
        months = tuple(config.get('extract.month').get('list.of.months'))
        logging.info("{} : Fetching data for months {}".format(
                datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), ', '.join(str(i) for i in months)))
        sales_df = get_sales_data(con, months)
        logging.info("{} : Sales data fetched for months {}".format(
            datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), ','.join(str(i) for i in months)))
        regions, customers = get_cust_and_regions(sales_df)
        logging.info("{} : Regions and customer IDs identified".format(
            datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')))
        cust_df = get_customer_data(con, tuple(customers))
        logging.info("{} : Customer data fetched for months {}".format(
            datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), ','.join(str(i) for i in months)))
        rgns_df = get_region_data(con, tuple(regions))
        logging.info("{} : Regions data fetched for months {}".format(
            datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), ','.join(str(i) for i in months)))
        write_data_to_csv(sales_df, rgns_df, cust_df)
        create_merged_sales_dump(sales_df, rgns_df, cust_df)
    elif mode == 'load' :
        # creating db session
        Session = sessionmaker(bind=con)
        session = Session()
        logging.info('{} : DB session for loading created successfully!'.format(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')))

        #read source file data
        file = DATA_PATH + SOURCE_FILE
        src_df = read_source_flat_sales_data(file)

        flag = load_sales_data(session, con,src_df)
        if flag == 1:
            logging.info('{} : Data load completed successfully!'.format(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')))
        else:
            logging.error('{} : Data load failed and rollback was performed!'.format(
                datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')))


    else:
        logging.error(
            "{} : Currently only pulling data from data sources which is assumed to be loaded from another application!".format(
                datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')))


    # Base.metadata.create_all()
