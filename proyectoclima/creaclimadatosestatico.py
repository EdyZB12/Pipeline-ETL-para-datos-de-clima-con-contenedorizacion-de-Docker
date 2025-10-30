from datetime import datetime, timedelta
from meteostat import Hourly, Point
import pandas as pd 
import logging 
import os

log_path = os.path.expanduser("C:/Users/zabu/Desktop/proyectoclima/logs")
os.makedirs(log_path, exist_ok=True)
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_path, "extract_weather.log")),
        logging.StreamHandler(),
    ]
)

def get_city_points():
    return{
        "Mexico City": Point(19.4326, -99.1332),
        "Madrid": Point(40.4168, -3.7038),
        "Toronto": Point(43.65107, -79.34015)
    }

def extract_weather_data(hours=24):
    cities =get_city_points()
    end=datetime.now()
    start = end - timedelta(hours=hours)

    all_data = []

    for city, location in cities.items():
        try: 
            logging.info(f"Extrayendo datos para {city} desde {start} hasta {end}")
            data = Hourly(location, start, end)
            df_city = data.fetch()
            if df_city.empty: 
                logging.warning(f"Sin datos disponibles para {city}")
                continue
        
            df_city["city"] = city
            df_city.reset_index(inplace=True)
            all_data.append(df_city)
    
        except Exception as e: 
            logging.error(f"Error extrayendo datos para {city}: {e}")
    
    if not all_data: 
        logging.error("No se extrajeron datos de ninguna ciudad")
        return pd.DataFrame()
    
    df_final = pd.concat(all_data, ignore_index=True)
    logging.info(f"Extracción completa: {len(df_final)} registros obtenidos")
    return df_final


if __name__ == "__main__":
    df_weather = extract_weather_data(hours=24)
    if not df_weather.empty:
        print(df_weather.head())
        df_weather.to_csv("C:/Users/zabu/Desktop/proyectoclima/data_raw_weather2.csv", index=False)
        logging.info("Datos guardados en data_raw_weather.csv")



