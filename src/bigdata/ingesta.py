import requests
import json
import sqlite3
import pandas as pd
import os
from datetime import datetime
from io import StringIO

class AnimeDataProcessor:
    """Clase para procesar datos de anime desde una API externa"""
    
    def __init__(self, base_url="https://api.jikan.moe/v4"):
        """
        Inicializa el procesador de datos de anime.
        
        Args:
            base_url (str): URL base de la API
        """
        self.base_url = base_url
        self.data = {}
        self.conn = None
        self.db_path = 'src/bigdata/static/db/anime_data.db'
        self.sample_path = 'src/bigdata/static/db/muestra_anime.xlsx'
        self.audit_path = 'src/bigdata/static/auditoria/auditoria.txt'
        
        # Crear directorio de datos si no existe
        #if not os.path.exists('db'):
        #    os.makedirs('db')

        # Crear estructura completa de directorios
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.audit_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.sample_path), exist_ok=True)
    
    def obtener_datos_api(self, params={}):
        """
        Obtiene datos de la API usando los parámetros proporcionados.
        
        Args:
            params (dict): Parámetros para la solicitud a la API
            
        Returns:
            dict: Datos obtenidos o diccionario vacío en caso de error
        """
        url = "{}/{}/{}/{}".format(
            self.base_url, 
            params["characters"], 
            params["{id}"], 
            params["anime"]
        )
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            self.data = response.json()
            return self.data
        except requests.exceptions.RequestException as error:
            print(f"Error al obtener datos del API: {error}")
            self.data = {}
            return {}
    
    def crear_base_datos(self):
        """
        Crea la base de datos SQLite para almacenar los datos de anime.
        """
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Crear tabla de personajes
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS personajes (
            id INTEGER PRIMARY KEY,
            nombre TEXT NOT NULL,
            nombre_kanji TEXT,
            role TEXT,
            anime_id INTEGER,
            imagen_url TEXT,
            fecha_extraccion TEXT
        )
        ''')
        
        self.conn.commit()
        print(f"Base de datos creada en {self.db_path}")
    
    def insertar_datos(self):
        """
        Inserta los datos obtenidos de la API en la base de datos.
        
        Returns:
            int: Número de registros insertados
        """
        if not self.conn:
            self.crear_base_datos()
            
        cursor = self.conn.cursor()
        registros_insertados = 0
        fecha_extraccion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # Verificar la estructura de los datos
            if 'data' in self.data and isinstance(self.data['data'], list):
                for personaje in self.data['data']:
                    cursor.execute('''
                    INSERT OR REPLACE INTO personajes 
                    (id, nombre, nombre_kanji, role, anime_id, imagen_url, fecha_extraccion)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        personaje.get('mal_id', None),
                        personaje.get('name', ''),
                        personaje.get('name_kanji', ''),
                        personaje.get('role', ''),
                        # Ajustar estas rutas según la estructura real de los datos
                        personaje.get('anime', {}).get('mal_id') if isinstance(personaje.get('anime'), dict) else None,
                        personaje.get('images', {}).get('jpg', {}).get('image_url', '') 
                        if isinstance(personaje.get('images'), dict) else '',
                        fecha_extraccion
                    ))
                    registros_insertados += 1
            
            self.conn.commit()
            print(f"Se insertaron {registros_insertados} registros en la base de datos")
        except Exception as e:
            self.conn.rollback()
            print(f"Error al insertar datos: {e}")
        
        return registros_insertados
    
    def generar_muestra(self):
        """
        Genera un archivo Excel con una muestra de los datos almacenados.
        """
        if not self.conn:
            print("No hay conexión a la base de datos")
            return
        
        # Obtener datos de la base de datos
        query = "SELECT * FROM personajes"
        df = pd.read_sql_query(query, self.conn)
        
        # Guardar en Excel
        df.to_excel(self.sample_path, index=False)
        print(f"Archivo de muestra generado en {self.sample_path}")
        
        return df
    
    def generar_auditoria(self):
        """
        Genera un archivo de auditoría comparando los datos de la API con los de la base de datos.
        """
        if not self.conn:
            print("No hay conexión a la base de datos")
            return
        
        # Obtener datos de la base de datos
        query = "SELECT * FROM personajes"
        df_db = pd.read_sql_query(query, self.conn)
        
        # Contar registros
        registros_api = len(self.data.get('data', [])) if 'data' in self.data else 0
        registros_db = len(df_db)
        
        # Generar reporte
        with open(self.audit_path, 'w', encoding='utf-8') as f:
            f.write("=== REPORTE DE AUDITORÍA ===\n")
            f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write(f"Registros obtenidos del API: {registros_api}\n")
            f.write(f"Registros almacenados en la base de datos: {registros_db}\n\n")
            
            if registros_api == registros_db:
                f.write("✓ ÉXITO: Todos los registros del API fueron almacenados correctamente.\n")
            else:
                f.write(f"⚠ ADVERTENCIA: Hay una diferencia de {abs(registros_api - registros_db)} registros entre el API y la base de datos.\n")
            
            # Verificar integridad
            if 'data' in self.data and registros_db > 0:
                api_ids = [item.get('mal_id') for item in self.data.get('data', []) if 'mal_id' in item]
                db_ids = df_db['id'].tolist()
                
                missing_ids = set(api_ids) - set(db_ids)
                
                if missing_ids:
                    f.write(f"\n⚠ ADVERTENCIA: Los siguientes IDs del API no están en la base de datos: {missing_ids}\n")
                else:
                    f.write("\n✓ ÉXITO: Todos los IDs del API están presentes en la base de datos.\n")
        
        print(f"Archivo de auditoría generado en {self.audit_path}")
    
    def procesar_datos_completo(self, params={}):
        """
        Ejecuta el proceso completo de ETL.
        
        Args:
            params (dict): Parámetros para la solicitud a la API
        """
        print("Iniciando proceso ETL...")
        
        # Extracción
        print("Extrayendo datos del API...")
        datos = self.obtener_datos_api(params)
        
        if not datos:
            print("No se pudo obtener datos del API. Finalizando proceso.")
            return False
        
        # Almacenamiento
        print("Creando base de datos...")
        self.crear_base_datos()
        
        print("Insertando datos en la base de datos...")
        self.insertar_datos()
        
        # Generación de evidencias
        print("Generando archivo de muestra...")
        self.generar_muestra()
        
        print("Generando archivo de auditoría...")
        self.generar_auditoria()
        
        print("Proceso ETL completado exitosamente.")
        return True
    
    def mostrar_registros(self):
        """
        Muestra los registros almacenados en la base de datos.
        """
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
            
        query = "SELECT * FROM personajes"
        df = pd.read_sql_query(query, self.conn)
        
        if len(df) > 0:
            print(f"\nRegistros en la base de datos ({len(df)} total):")
            print(df.to_string(index=False))
        else:
            print("No hay registros en la base de datos.")
        
        return df
    
    def cerrar_conexion(self):
        """
        Cierra la conexión a la base de datos.
        """
        if self.conn:
            self.conn.close()
            print("Conexión a la base de datos cerrada.")


# Ejecución del código cuando se llama directamente al script
if __name__ == "__main__":
    # Parámetros para la API
    parametros = {"characters":"characters","{id}":"20","anime":"anime"}
    
    # Crear instancia del procesador
    procesador = AnimeDataProcessor()
    
    try:
        # Ejecutar el proceso completo
        procesador.procesar_datos_completo(parametros)

        # Mostrar registros
        procesador.mostrar_registros()
    finally:
        # Asegurar que la conexión se cierre correctamente
        procesador.cerrar_conexion()