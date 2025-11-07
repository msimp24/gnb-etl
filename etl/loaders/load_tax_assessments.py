from pathlib import Path
import sqlite3
import logging
import os
from dotenv import load_dotenv

from etl.transformers.transform_assessments import trasform_tax_assessments
load_dotenv()

ENV_DB_PATH = os.environ.get('DB_PATH')

BASE_DIR = Path(__file__).resolve().parent.parent.parent

if ENV_DB_PATH and Path(ENV_DB_PATH).is_absolute():
    DB_PATH = Path(ENV_DB_PATH)
else:
    DB_PATH = BASE_DIR / ENV_DB_PATH
    
LOG_FILE = BASE_DIR / 'logs' /'load_data_log.log'

LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    filemode='a'  
)
logger = logging.getLogger()    


def load_tax_assessments():
  df = trasform_tax_assessments()

  try:
      DB_PATH.parent.mkdir(parents=True, exist_ok=True)  
      with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(con=conn, name="property_assessments", if_exists='replace')

      logger.info(f"Data loaded successfully. {len(df)} rows written.") 
  
  except Exception as e:
      logger.error(f"An error occurred while loading data: {e}")
      
      
if __name__ == '__main__':
  load_tax_assessments()