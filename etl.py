import pandas as pd
import requests
import utils
from datetime import datetime

class ETL:
    BASE_URL = "https://api.openweathermap.org/data/2.5/weather?q"
    SAVE_PATH = "data/"
    
    def __init__(self, city_name:str, api_key:str) -> None:
        self.url =  f"{self.BASE_URL}={city_name}&appid={api_key}"
        self.city_name = city_name
        pass

    def extract(self):
        return requests.get(self.url).json()

    def transform(self, data):
            return pd.DataFrame(
                [{
                'City' : data['name'],
                'Weather' : data["weather"][0]['description'],
                'Temperature (C)' : utils.kelvin_to_celsius(data['main']['temp']),
                'Feels Like (C)' : utils.kelvin_to_celsius(data['main']['feels_like']),
                'Minimun Temperature (C)' : utils.kelvin_to_celsius(data['main']['temp_min']),
                'Maximun Temperature (C)' : utils.kelvin_to_celsius(data['main']['temp_max']),
                'Pressure' : data["main"]['pressure'],
                'Humidity' : data["main"]['humidity'],
                'Sea Level' : data["main"]['sea_level'],
                'Speed' : data["wind"]['speed'],
                'Visibility' : data['visibility'],
                'Time of Record' : datetime.utcfromtimestamp(data['dt'] + data['timezone']),
                'Sunrise (Local Time)' : datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']),
                'Sunset (Local Time)' : datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone']) ,
            }]
        )
    
    def load(self, transformed_data):
        file_path = f'{self.SAVE_PATH}{self.city_name}_weather-report_{datetime.now().timestamp()}.csv'
        transformed_data.to_csv(file_path, index=False)  