from etl.extracters.extract_tax_assessments import extract_tax_assessments

def trasform_tax_assessments():
  df = extract_tax_assessments()  
  return df

if __name__ == '__main__':
  trasform_tax_assessments()  