import logging
import config
from etl import ETL

def openweather_etl():
    etl = ETL('Imbituba', config.api_key)

    try:
        logging.info('Initiating extraction...')
        data = etl.extract()

        logging.info('Extraction COMPLETE. Transforming Data...')
        transformed_data = etl.transform(data)

        logging.info('Data transformation COMPLETE. Saving Data')
        etl.load(transformed_data)
    
    except Exception as e:
        logging.error(f"Execução falhou pelo motivo: {e}")


if __name__ == '__main__':
    openweather_etl()
    




