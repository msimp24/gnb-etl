from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_FILE = BASE_DIR / 'data' / 'raw' / 'TA.tsv'

def extract_code_data():
    try:
        df = pd.read_csv(RAW_FILE, sep='\t', encoding='utf-8')
        
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(RAW_FILE, sep='\t', encoding='latin-1')
        except Exception as e:
            raise IOError(f"Failed to read {RAW_FILE}. Try encoding 'latin-1' or 'windows-1252'. Original error: {e}")
    
    headers = ['tax_code', 'tax_areas']      
    df.columns = headers
    
    return df      



if __name__ == '__main__':
    df = extract_code_data(RAW_FILE)  
    print(df.head())