from etl.extracters.extract_taxareas import extract_code_data

def clean_code_data(df):
  return df


def transform_codes():
  df = extract_code_data()
  return df

if __name__ == '__main__':
  transform_codes()
  