print("zabusito")

import pandas as pd
import numpy as np
import csv
import logging
import os 
from sqlalchemy import create_engine 
import psycopg2
from psycopg2.extras import execute_values 
from dotenv import load_dotenv
load_dotenv()

#datos = r"C:\Users\zabu\Desktop\proyectoclima\data_raw_weather.csv"
#datos_limpios = r"C:\Users\zabu\Desktop\proyectoclima\data_clean_weather.csv"

datos = "/app/data/data_raw_weather.csv"
datos_limpios = "/app/output/data_clean_weather.csv"


df = pd.read_csv(datos)
print(df.head())


#base de datos 

log_dir = r'C:\app\logs'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
   level=logging.INFO, 
   format='%(asctime)s - %(levelname)s - %(message)s',
   handlers = [
       logging.FileHandler(os.path.join(log_dir,'pipeline.log')),
       logging.StreamHandler()
    ]
)

DB_CONFIG = {
   'host': os.getenv('DB_HOST', 'localhost'),
   'port': int(os.getenv('DB_PORT', 5432)),
   'user': os.getenv('DB_USER', 'postgres'),
   'password': os.getenv('DB_PASSWORD', 'TMHzabusito88'),
   'database': os.getenv('DB_NAME', 'weather_data')
}


def pipeline(datos, datos_limpios):
    
    #conversion de tipo: 

    try: 

        df = pd.read_csv(datos)

        col_numerica = ['temp','dwpt', 'rhum', 'prcp',
                        'snow', 'wdir', 'wspd', 'pres', 'tsun', 'coco']
        for col in col_numerica: 
            df[col] = pd.to_numeric(df[col], errors = 'coerce')
    
    #identifica datos faltantes

        miss_temp = df['temp'].isnull().sum()
        if miss_temp > 0: 
            logging.warning(f"temp is not valid: {miss_temp}")
    
        miss_dwpt = df['dwpt'].isnull().sum()
        if miss_dwpt > 0: 
            logging.warning(f"dwpt is not valid: {miss_dwpt}")
    
        miss_rhum = df['rhum'].isnull().sum()
        if miss_rhum > 0: 
           logging.warning(f"rhum is not valid: {miss_rhum}")
    
        miss_prcp = df['prcp'].isnull().sum()
        if miss_prcp > 0: 
           logging.warning(f"prcp is not valid: {miss_prcp}")
    
        miss_snow = df['snow'].isnull().sum()
        if miss_snow > 0: 
           logging.warning(f"snow is not valid: {miss_snow}")
    
        miss_wdir = df['wdir'].isnull().sum()
        if miss_wdir > 0: 
           logging.warning(f"wdir is not valid: {miss_wdir}")

        miss_wspd = df['wspd'].isnull().sum()
        if miss_wspd > 0: 
            logging.warning(f"wspd is not valid: {miss_wspd}")
    
        miss_wpgt = df['wpgt'].isnull().sum()
        if miss_wpgt > 0: 
           logging.warning(f"wpgt is not valid: {miss_wpgt}")
    
        miss_pres = df['pres'].isnull().sum()
        if miss_pres > 0: 
           logging.warning(f"pres is not valid: {miss_pres}")
        
        miss_tsun = df['tsun'].isnull().sum()
        if miss_tsun > 0: 
           logging.warning(f"tsun is not valid: {miss_tsun}")
        
        miss_coco = df['coco'].isnull().sum()
        if miss_coco > 0: 
           logging.warning(f"coco is not valid: {miss_coco}")
        
        miss_city = df['city'].isnull().sum()
        if miss_city > 0: 
           logging.warning(f"city is not valid: {miss_city}")

    #rellenamos la columna snow con 0 ya que no esta nevando ahí

        df['snow'] = df['snow'].fillna(0)

    #lo mismo hacemos pero con la columa tsun, la hora de sol 
    #en minutos 

        df['tsun'] = df['tsun'].fillna(0)

    # y lo mismo para wpgt, la rafaga de viento 
       
        df['wpgt'] = df['wpgt'].fillna(0)
 
    #separamos la columa time ya que tiene la fecha y el tiempo registrados
    #en una sola columna 

        partes_time = df['time'].str.split(' ', expand = True)

        df['Date'] = partes_time[0]
        df['Times'] =partes_time[1]

    #vamos a convertir de grados a Farenheint
        
        #para Farenheint
        df['Temp_Fahrenheint'] = (df['temp']*1.8) + 32

        #para Kelvin en función de los farenheint

        df['temp_Kelvin'] = (5/9)*(df['Temp_Fahrenheint']-32) + 273.15

    #vamos abreviar a las ciudades

        CY = df['city']

        def abreviacion(CY):
           if 'Mexico City' in CY: return 'CDMX'
           elif 'Toronto' in CY: return 'TN'
           elif 'Madrid' in CY: return 'MD' 
           else: 
            return 'Desconocido'
           
        df['city_abrevi'] = CY.apply(abreviacion)

    #el significado de coco esta en funcion del code, este nos indica
    # la condicion del clima dado un número entero, como sigue:


        COCO = df['coco']

        def WC(COCO):
           if COCO == 1: return 'Clear'
           elif COCO == 2: return 'Fair'
           elif COCO == 3: return 'Cloudy'
           elif COCO == 4: return 'Nublado'
           elif COCO == 5: return 'Fog'
           elif COCO == 6: return 'Freezing Fog'
           elif COCO == 7: return 'Light Rain'
           elif COCO == 8: return 'Rain'
           elif COCO == 9: return 'Heavy Rain'
           elif COCO == 10: return 'Freezing Rain'
           elif COCO == 11: return 'Heavy Freezing Rain'
           elif COCO == 12: return 'Sleet'
           elif COCO == 13: return 'Heavy Sleet'
           elif COCO == 14: return 'Light Snowfall'
           elif COCO == 15: return 'Snowfall'
           elif COCO == 16: return 'Heavy Snowfall'
           elif COCO == 17: return 'Rain Shower'
           elif COCO == 18: return 'Heavy Rain Shower'
           elif COCO == 19: return 'Sleet Shower'
           elif COCO == 20: return 'Heavy Sleet Shower'
           elif COCO == 21: return 'Show Shower'
           elif COCO == 22: return 'Heavy Snow Shower'
           elif COCO == 23: return 'Lightning'
           elif COCO == 24: return 'Hail'
           elif COCO == 25: return 'Thunderstorm'
           elif COCO == 26: return 'Heavy Thunderstorm'
           elif COCO == 27: return 'Storm'
           else: 
               return 'Desconocido' 

        df['Weather_condition'] = COCO.apply(WC)
       
        #vamos a convertior los km/h a m/s 
        
        df['wspd_ms'] = df['wspd']*(5/18)

        df['wpgt_ms'] = df['wpgt']*(5/18)

        #vamos a convertir los total de precipitacion de una hora a metros 

        df['prcp_m'] = df['prcp']*(0.001)

        #para la nieve 

        df['snow_m'] = df['snow']*(0.001)

        #y aqui vamos a convertir lo hectopascales a pascales 

        df['pres_Pa'] = df['pres']*100

        ##########################
        #creando base de datos
        ##########################

        def crear_base_dato():
           try: 
              conn = psycopg2.connect(
                 host= DB_CONFIG['host'],
                 port= DB_CONFIG['port'],
                 user=DB_CONFIG['user'],
                 password = DB_CONFIG['password'],
                 dbname = 'postgres'

              )
              conn.autocommit = True
              cursor = conn.cursor()

              cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_CONFIG['database']}';")
              exists = cursor.fetchone()

              if not exists: 
                 cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']};")
                 print(f"Base de datos '{DB_CONFIG['database']}' creada con exito")
              else: 
                 print(f"La base de datos '{DB_CONFIG['database']}' ya existe")
                
              cursor.close()
              conn.close()

           except psycopg2.Error as e: 
              if "already exists" in str(e):
                 print(f"La base de datos {DB_CONFIG['database']} ya existe")
              else: 
                 print(f"error creando base de datos: {e}")
        
       #creamos las columnas de la base de datos
        
        def crear_columnas():
           try: 
              conn =psycopg2.connect(**DB_CONFIG)
              cursor = conn.cursor()
              create_table_query = """
              CREATE TABLE IF NOT EXISTS weather_data(
                Date DATE, 
                Times TIME, 
                Temp_Fahrenheint NUMERIC, 
                temp_Kelvin NUMERIC, 
                city_abrevi VARCHAR(100),
                Weather_condition VARCHAR(100),
                wspd_ms NUMERIC,
                wpgt_ms NUMERIC, 
                prcp_m NUMERIC, 
                snow_m NUMERIC, 
                pres_PA INTEGER
              );
              """
              cursor.execute(create_table_query)
              cursor.execute("""
              CREATE INDEX IF NOT EXISTS idx_weather_data ON weather_data(pres_PA);
              """)

              conn.commit()
              print("Columnas creadas con exito")

           except psycopg2.Error as e: 
              print(f"error al crear las columnas: {e}")
        
        def carga_datos_posgres(df):
           try: 
              engine = create_engine(
                 f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                 f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
              )

              df.to_sql(
                 name = 'weather_data',
                 con=engine, 
                 if_exists='replace',
                 index=False,
                 method='multi',
                 chunksize=1000
              )

              print("Datos cargas con existo a PosgreSQL")

           except Exception as e: 
              print(f"Error cargando datos a PosgreSQL: {e}")
              return False

        #crea el csv limpio 
        df.to_csv(datos_limpios, index=False, encoding='utf-8')

        crear_base_dato()
        crear_columnas()
        carga_datos_posgres(df)

        return True 
    
    except Exception as e: 
        print(f"error: {e}")
        return False

if __name__ == "__main__":
    pipeline(datos, datos_limpios)