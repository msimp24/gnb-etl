from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_FILE = BASE_DIR / 'data' / 'raw' / 'Assessment.tsv'

def extract_tax_assessments():
    try:
        df = pd.read_csv(RAW_FILE, sep='\t', encoding='utf-8')
        
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(RAW_FILE, sep='\t', encoding='latin-1')
        except Exception as e:
            raise IOError(f"Failed to read {RAW_FILE}. Try encoding 'latin-1' or 'windows-1252'. Original error: {e}")
    
    new_headers = ['p_id', 'year', 'prop_tax_evaluation', 'tax_amount']
    last_column_name = df.columns[-1]

    df = df.drop(columns=[last_column_name])
    df.columns = new_headers
    return df     



if __name__ == '__main__':
    df = extract_tax_assessments(RAW_FILE)  
    print(df.head())