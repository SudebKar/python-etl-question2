import unittest

from sqlalchemy.orm import sessionmaker

from utils.connections import DbConnect
from utils.custom_util import parse_config,get_sales_data,get_cust_and_regions,get_customer_data,get_region_data,\
    read_source_flat_sales_data, load_sales_data
from utils.metadata_util import Base, Region,Customer,Sales
import pandas as pd
from datetime import date


class TestApp(unittest.TestCase):
    """Class contains different methods to test each and every functionality of application in bits and pieces
    while development phase, the test case values need to be changed in test cases as needed"""

    def setUp(self):
        self.config = parse_config('../config/test_config.yml')
        self.conf = self.config.get('db.config')
        self.db = DbConnect(self.conf.get('driver'), self.conf.get('host'), self.conf.get('port')
                            , self.conf.get('username'), self.conf.get('password'), self.conf.get('database'))
        self.conn = self.db.create_sql_engine()
        Session = sessionmaker(bind=self.conn)
        self.session = Session()


    def test_connection(self):
        self.assertIsNotNone(self.conn,msg="Database connection fails!")

    def test_metadata(self):
        tables = Base.metadata.tables.keys()
        self.assertEqual(len(tables), 3, msg="All 3 tables metadata not created in python!")

    def test_sales_data_by_month(self):
        months = tuple(parse_config('../config/test_config.yml').get('extract.month').get('list.of.months'))
        sales_df = get_sales_data(self.conn,months)
        self.assertEqual(len(sales_df),3,msg="Number or records returned not matching!")

    def test_cust_regs_for_sales(self):
        months = tuple(parse_config('../config/test_config.yml').get('extract.month').get('list.of.months'))
        sales_df = get_sales_data(self.conn,months)
        regions,customers = get_cust_and_regions(sales_df)
        #Hard coding done for testing
        self.assertTrue((sorted(regions) == [6,7,8]) and (sorted(customers) == [4,5,6])\
                        ,msg="Not fetching region and customer ids as expected!")

    def test_related_cust_to_sales(self):
        customers = (5,6)
        cust_df = get_customer_data(self.conn,customers)
        #Hard coding done for testing
        self.assertTrue(sorted(list(cust_df["customer_name"])) == ['cust2','cust3'],msg="Customer details not pulled correctly!")

    def test_related_rgns_to_sales(self):
        regions = (8,6)
        rgns_df = get_region_data(self.conn,regions)
        #Hard coding done for testing
        self.assertTrue(sorted(list(rgns_df["region_name"])) == ['east','south'],msg="Region details not pulled correctly!")

    def test_read_from_file(self):
        src_df = read_source_flat_sales_data('../data/Sales_Source.csv')
        self.assertEquals(len(src_df),4,msg="Read from source file is unsuccessful!")

    def test_load_into_db(self):
        df = pd.DataFrame([{"sales_date" : date(2021,1,10),
                           "sales_amount" : 1000,
                           "customer_name" : "Siraj",
                           "customer_address" : "Secunderabad",
                           "customer_phone" : "9827354253",
                           "region_name" : "Central",
                           "country" : "India"}])

        flag = load_sales_data(self.session,self.conn,df)

        self.assertEqual(flag,1,msg="Database load was not successful!")




if __name__ == '__main__':
    unittest.main()
