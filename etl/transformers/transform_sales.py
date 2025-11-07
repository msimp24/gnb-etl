from etl.extracters.extract_sales import extract_sales_data
from etl.transformers.transform_master import transform_master
from etl.transformers.transform_assessments import trasform_tax_assessments
from etl.transformers.transform_codes import transform_codes

import pandas as pd

def clean_sales_data(df):
  df['sale_date'] = pd.to_datetime(df['sale_date']) 
  df['year'] = df['sale_date'].dt.year 
  df['month'] = df['sale_date'].dt.month
  df['sale_date'] = df['sale_date'].dt.strftime('%Y-%m-%d')
  df['is_market_sale'] = df['sale_price'].apply(lambda x: True if x > 1 else False)
  
  return df

def merge_sales_master(sales_df, master_df, assess_df, codes_df):
  
  merged_df = pd.merge(sales_df, master_df, on='p_id', how='inner')
  
  merged_df = merged_df.drop(['tax_year', 'prop_tax_evaluation', 'prop_tax_amount'], axis=1)
  
  merged_df_v2 = pd.merge(merged_df, assess_df, on=['p_id', 'year'], how="left")
  
  final_merged_df = pd.merge(
    merged_df_v2,
    codes_df,
    on='tax_code',
    how='left'  
)
  
  final_merged_df = final_merged_df.fillna('N/A')
  
  
  
  return final_merged_df  

def aggregate_sales_data(merged_df):
  merged_df = merged_df[merged_df['is_market_sale']]
  
  year_df = merged_df.groupby(['year', 'tax_code']).agg(
    median_sale_price=('sale_price', 'median'),
    average_sale_price=('sale_price', 'mean'), 
    total_sales_count=('p_id', 'count')
).reset_index()
  
  year_df['year'] = year_df['year'].astype(str)
  year_df['granularity_level'] = "Y"
  
  year_df = year_df.rename(columns={'year' : 'time_period'})
  
  merged_df['year_month'] = pd.to_datetime(merged_df['sale_date']).dt.strftime('%Y-%m')
  merged_df = merged_df[merged_df['is_market_sale']].copy()
  
  month_df = merged_df.groupby(['year_month', 'tax_code']).agg(
  median_sale_price=('sale_price', 'median'),
  average_sale_price=('sale_price', 'mean'), 
  total_sales_count=('p_id', 'count')
).reset_index()
  
  month_df['year_month'] = month_df['year_month'].astype(str)
  month_df['granularity_level'] = "M"
  
  month_df = month_df.rename(columns={'year_month':'time_period'})
  
  year_all_df = merged_df.groupby(['year']).agg(
    median_sale_price = ('sale_price', 'median'),
    average_sale_price = ('sale_price', 'mean'),
    total_sales_count = ('p_id', 'count')
  ).reset_index()
  
  year_all_df['year'] = year_all_df['year'].astype(str)
  year_all_df = year_all_df.rename(columns={'year' : 'time_period'})
  
  year_all_df['granularity_level'] = "Y"
  year_all_df['tax_code'] = 'All'
  
  month_all_df = merged_df.groupby(['year_month']).agg(
  median_sale_price = ('sale_price', 'median'),
  average_sale_price = ('sale_price', 'mean'),
  total_sales_count = ('p_id', 'count')
  ).reset_index()
  
  month_all_df['year_month'] = month_all_df['year_month'].astype(str)
  month_all_df = month_all_df.rename(columns={'year_month' : 'time_period'})
  
  month_all_df['granularity_level'] = "M"
  month_all_df['tax_code'] = 'All'
  
  fact_market_summary_df = pd.concat([year_df,month_df, year_all_df, month_all_df])
  fact_market_summary_df['average_sale_price'] = fact_market_summary_df['average_sale_price'].round(2)
  
  return fact_market_summary_df


def transform_sales_data():
  df = extract_sales_data() 
  df = clean_sales_data(df)
  master_df = transform_master()
  assess_df = trasform_tax_assessments()
  codes_df = transform_codes()
  merged_df = merge_sales_master(df, master_df,assess_df, codes_df)
  return merged_df

def transform_market_summary():
  df = extract_sales_data() 
  df = clean_sales_data(df)
  master_df = transform_master()
  assess_df = trasform_tax_assessments()
  codes_df = transform_codes()
  merged_df = merge_sales_master(df, master_df,assess_df, codes_df)
  fact_summary_df = aggregate_sales_data(merged_df)
  
  
  return fact_summary_df

if __name__  == '__main__':
  df = transform_sales_data()  
  print(df.head())

  