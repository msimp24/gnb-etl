from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_FILE = BASE_DIR / 'data' / 'raw' / 'tombstone.tsv'

def extract_master_data():
    try:
        df = pd.read_csv(RAW_FILE, sep='\t', encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(RAW_FILE, sep='\t', encoding='latin-1')
        except Exception as e:
            raise IOError(f"Failed to read {RAW_FILE}. Try encoding 'latin-1' or 'windows-1252'. Original error: {e}")
    
    headers = ['p_id', 'address', 'tax_code', 'description', 'tax_year', 'prop_tax_evaluation', 'prop_tax_amount']  
    
    df.columns = headers
    return df      


if __name__ == '__main__':
    df = extract_master_data(RAW_FILE)

