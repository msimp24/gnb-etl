from etl.extracters.extract_master import extract_master_data

def clean_master_data(df):
  
  return df

def transform_master():
  df = extract_master_data()
  return df

if __name__ == '__main__':
  df = transform_master() 
  print(df)