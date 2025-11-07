from etl.loaders.load_codes import load_codes
from etl.loaders.load_sales import load_sales_data
from etl.loaders.load_tax_assessments import load_tax_assessments
from etl.loaders.load_fact_market import load_fact_market

def load_data_tables():
  load_codes()
  load_sales_data()
  load_tax_assessments()
  load_fact_market()
  

if __name__ =="__main__":
  load_data_tables()  