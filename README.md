**What is this?**

This is a simple python utility that helps us to pull Sales data and related Region and Customer data from database that has been loaded by another application.
This creates simple csv dumps for the months for which data needs to be pulled from sales, region and customer tables in database.
Using unittest framework to closely test each of the methods for respective functionalities.


**How this works?**

_Input :_

Under config folder in config.yml file:

        - Specify the database configurations
        - Specify months for which data is needed (Year assumed to be 2020)
        - In the __init__.py file under utils package update the sales, customer, region table names and schemas and output filenames which would store the data after pulling from database.
        - If 'pull' option is selected in the config file as mode of operation, 
                then data is picked up from database and csv dumps as well as consolidated/merged sql dump is created 
        - If 'load' is selected in the config file as mode of operation,
                then data is picked up from a source sales csv file and loaded into the database 
                by maintaining the relationship created in sqlalchemy ORM
        

_Metadata_ :
        
        - Metadata for each of the tables has been defined using class hierarchies have been maintained in the metadata_util under utils package
        - SQLAlchemy has been used for maintaining metadata and relations.
    
_Test_ :

        - Under test folder all the testcases have been defined to test the following :
            - Database connectivity
            - Metadata creation check
            - Sales data fetch from datbase based on months specified
            - Related customer id and region id fetch
            - Related customer data set check
            - Related region data set check
        - Test config is also maintained test foler

