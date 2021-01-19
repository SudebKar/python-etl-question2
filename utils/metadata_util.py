from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData, Column, Integer, DECIMAL, ForeignKey, String, DateTime, Sequence
from sqlalchemy.orm import relationship, backref
from utils import SALES_TABLE, REGION_TABLE, CUSTOMER_TABLE, SOURCE_SCHEMA

metadata = MetaData(schema=SOURCE_SCHEMA)
Base = declarative_base(metadata = metadata)


class Region(Base):
    __tablename__ = REGION_TABLE
    region_id = Column(Integer, Sequence("reqion_id_seq"), primary_key=True)
    region_name = Column(String)
    country = Column(String)

    def __init__(self, region_name, country):
        self.region_name = region_name
        self.country = country

    def __repr__(self):
        return "<Region(region_id='%s', region_name='%s', country='%s')>" % (
        self.region_id, self.region_name, self.country)


class Customer(Base):
    __tablename__ = CUSTOMER_TABLE
    customer_id = Column(Integer, Sequence("customer_id_seq"), primary_key=True)
    customer_name = Column(String)
    customer_phone = Column(String)
    customer_address = Column(String)

    def __init__(self, customer_name, customer_phone, customer_address):
        self.customer_name = customer_name
        self.customer_phone = customer_phone
        self.customer_address = customer_address

    def __repr__(self):
        return "<Customer(customer_id='%s', customer_name='%s', customer_phone='%s',customer_address='%s')>" % (
        self.customer_id, self.customer_name, self.customer_phone, self.customer_address)


class Sales(Base):
    __tablename__ = SALES_TABLE
    sales_id = Column(Integer, Sequence("sales_id_seq"), primary_key=True)
    sales_amount = Column(DECIMAL)
    sales_date = Column(DateTime)
    region_id = Column(Integer, ForeignKey('test_region.region_id'))
    customer_id = Column(Integer, ForeignKey('test_customer.customer_id'))

    customer = relationship("Customer", backref=SALES_TABLE)
    region = relationship("Region", backref=SALES_TABLE)

    def __init__(self, sales_amount, sales_date, customer, region):
        self.sales_amount = sales_amount
        self.sales_date = sales_date
        self.customer = customer
        self.region = region

    def __repr__(self):
        return "<Sales(sales_id='%s', sales_amount='%s', sales_date='%s',region_id='%s',customer_id='%s')>" % (
        self.sales_id, self.sales_amount, self.sales_date, self.region_id, self.customer_id)
