import requests
import json
import sqlite3
import pandas as pd
import numpy as np
import os
import random
from datetime import datetime
from io import StringIO

class AnimeDataProcessor:
    """Clase para procesar datos de anime desde una API externa e insertar datos con errores controlados"""
    
    def __init__(self, base_url="https://api.jikan.moe/v4", add_errors=True, error_rate=0.3):
        """
        Inicializa el procesador de datos de anime.
        
        Args:
            base_url (str): URL base de la API
            add_errors (bool): Si se deben agregar errores controlados a los datos
            error_rate (float): Tasa de error (0.0 a 1.0)
        """
        self.base_url = base_url
        self.data = {}
        self.conn = None
        self.db_path = 'src/bigdata/static/db/anime_data.db'
        self.sample_path = 'src/bigdata/static/db/muestra_anime.xlsx'
        self.audit_path = 'src/bigdata/static/auditoria/auditoria.txt'
        self.add_errors = add_errors
        self.error_rate = error_rate
        
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
        # Guardar los parámetros para uso posterior
        self.params = params
        
        # Construir URL correcta para la API
        url = f"{self.base_url}/anime/{params.get('{id}', '')}/characters"
        
        print(f"Consultando URL: {url}")
        
        try:
            # Añadir timeout para evitar que se quede colgado indefinidamente
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            print(f"Respuesta recibida correctamente. Código: {response.status_code}")
            
            data = response.json()
            print(f"Datos JSON procesados. Personajes encontrados: {len(data.get('data', []))}")
            
            self.data = data
            return data
            
        except requests.exceptions.Timeout:
            print(f"ERROR: Timeout al conectar con la API: {url}")
        except requests.exceptions.ConnectionError:
            print(f"ERROR: Problema de conexión con la API: {url}")
        except requests.exceptions.HTTPError as err:
            print(f"ERROR HTTP: {err}")
        except json.JSONDecodeError:
            print(f"ERROR: No se pudo decodificar la respuesta como JSON")
        except Exception as e:
            print(f"ERROR INESPERADO: {e}")
        
        # Si llegamos aquí es porque hubo un error
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
        
        # Crear tabla para actores de voz
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS voice_actors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER NOT NULL,
            person_name TEXT NOT NULL,
            language TEXT NOT NULL,
            character_id INTEGER NOT NULL,
            image_url TEXT,
            fecha_extraccion TEXT,
            FOREIGN KEY (character_id) REFERENCES personajes (id)
        )
        ''')
        
        self.conn.commit()
        print(f"Base de datos creada en {self.db_path}")
    
    def _introducir_errores_personaje(self, character_data, error_probability):
        """
        Introduce errores controlados en los datos de un personaje
        
        Args:
            character_data (dict): Datos del personaje
            error_probability (float): Probabilidad de introducir error (0.0 a 1.0)
            
        Returns:
            dict: Datos del personaje con posibles errores
        """
        # Copia los datos para no modificar los originales
        data = character_data.copy()
        
        # Decidir si se introduce algún error en este registro
        if random.random() > error_probability:
            return data  # No introducir error
        
        # Seleccionar un tipo de error aleatorio para este registro
        error_type = random.choice([
            'null_value',
            'wrong_type',
            'special_chars',
            'date_format',
            'outlier'
        ])
        
        # Aplicar el error seleccionado
        if error_type == 'null_value':
            # Introducir NULL en un campo aleatorio
            field = random.choice(['nombre', 'nombre_kanji', 'role', 'imagen_url'])
            data[field] = None
            
        elif error_type == 'wrong_type':
            # Cambiar tipo de datos de anime_id a string
            data['anime_id'] = f"anime-{data['anime_id']}"
            
        elif error_type == 'special_chars':
            # Añadir caracteres especiales al nombre
            if 'nombre' in data and data['nombre']:
                data['nombre'] = f"{data['nombre']}#$%&*"
                
        elif error_type == 'date_format':
            # Cambiar formato de fecha
            data['fecha_extraccion'] = f"{random.randint(1, 31)}/{random.randint(1, 12)}/202{random.randint(0, 3)}"
            
        elif error_type == 'outlier':
            # Introducir outlier en anime_id
            data['anime_id'] = random.randint(9000000, 9999999)
        
        return data
    
    def insertar_datos(self):
        """
        Inserta los datos obtenidos de la API en la base de datos
        con la opción de introducir errores controlados.
        
        Returns:
            int: Número de registros insertados
        """
        if not self.conn:
            self.crear_base_datos()
                
        cursor = self.conn.cursor()
        registros_insertados = 0
        voice_actors_insertados = 0
        fecha_extraccion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Datos para duplicación
        duplicates_to_add = []
        
        try:
            # Verificar la estructura de los datos
            if 'data' in self.data and isinstance(self.data['data'], list):
                total_personajes = len(self.data['data'])
                print(f"Procesando {total_personajes} personajes para insertar en BD")
                
                # Obtener ID del anime de la URL
                anime_id = self.params.get('{id}')
                
                for idx, personaje in enumerate(self.data['data']):
                    try:
                        # Extraer información del personaje
                        character_data = personaje.get('character', {})
                        character_id = character_data.get('mal_id')
                        character_name = character_data.get('name', '')
                        character_image = character_data.get('images', {}).get('jpg', {}).get('image_url', '')
                        role = personaje.get('role', '')
                        
                        # Preparar datos del personaje
                        char_data = {
                            'id': character_id,
                            'nombre': character_name,
                            'nombre_kanji': None,  # No disponible en la respuesta actual
                            'role': role,
                            'anime_id': anime_id,
                            'imagen_url': character_image,
                            'fecha_extraccion': fecha_extraccion
                        }
                        
                        # Introducir errores controlados si está habilitado
                        if self.add_errors:
                            # Determinar si este registro se duplicará (aprox. 10% de los registros)
                            if random.random() < 0.1:
                                duplicate = char_data.copy()
                                duplicate['id'] = character_id + 10000  # Modificar ID para evitar conflicto de clave
                                duplicates_to_add.append(duplicate)
                            
                            # Introducir errores en los campos
                            char_data = self._introducir_errores_personaje(char_data, self.error_rate)
                        
                        # Debug para los primeros personajes
                        if idx < 2:
                            print(f"Procesando: ID={char_data['id']}, Nombre={char_data['nombre']}, Role={char_data['role']}, Anime={char_data['anime_id']}")
                        
                        # Insertar en la base de datos
                        cursor.execute('''
                        INSERT OR REPLACE INTO personajes 
                        (id, nombre, nombre_kanji, role, anime_id, imagen_url, fecha_extraccion)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            char_data['id'],
                            char_data['nombre'],
                            char_data['nombre_kanji'],
                            char_data['role'],
                            char_data['anime_id'],
                            char_data['imagen_url'],
                            char_data['fecha_extraccion']
                        ))
                        registros_insertados += 1
                        
                        # Procesar actores de voz
                        for voice_actor in personaje.get('voice_actors', []):
                            try:
                                person_data = voice_actor.get('person', {})
                                person_id = person_data.get('mal_id')
                                person_name = person_data.get('name', '')
                                language = voice_actor.get('language', '')
                                image_url = person_data.get('images', {}).get('jpg', {}).get('image_url', '')
                                
                                cursor.execute('''
                                INSERT INTO voice_actors 
                                (person_id, person_name, language, character_id, image_url, fecha_extraccion)
                                VALUES (?, ?, ?, ?, ?, ?)
                                ''', (
                                    person_id,
                                    person_name,
                                    language,
                                    character_id,
                                    image_url,
                                    fecha_extraccion
                                ))
                                voice_actors_insertados += 1
                            except Exception as e:
                                print(f"ERROR al insertar actor de voz: {e}")
                    
                    except Exception as e:
                        print(f"ERROR al insertar personaje {idx}: {e}")
                
                # Insertar los registros duplicados
                if duplicates_to_add:
                    print(f"Añadiendo {len(duplicates_to_add)} registros duplicados con errores...")
                    
                    for dup in duplicates_to_add:
                        try:
                            cursor.execute('''
                            INSERT OR REPLACE INTO personajes 
                            (id, nombre, nombre_kanji, role, anime_id, imagen_url, fecha_extraccion)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                dup['id'],
                                dup['nombre'],
                                dup['nombre_kanji'],
                                dup['role'],
                                dup['anime_id'],
                                dup['imagen_url'],
                                dup['fecha_extraccion']
                            ))
                            registros_insertados += 1
                        except Exception as e:
                            print(f"ERROR al insertar duplicado: {e}")
                
                self.conn.commit()
                print(f"Se insertaron {registros_insertados} registros en la base de datos")
                print(f"Se insertaron {voice_actors_insertados} actores de voz en la base de datos")
            else:
                print("No hay datos en formato esperado para insertar")
                
        except Exception as e:
            self.conn.rollback()
            print(f"ERROR general al insertar datos: {e}")
            
        return registros_insertados
    
    def generar_muestra(self):
        """
        Genera un archivo Excel con una muestra de los datos almacenados.
        Maneja errores de permisos intentando nombres de archivo alternativos.
        """
        if not self.conn:
            print("No hay conexión a la base de datos")
            return
            
        # Obtener datos de personajes
        query_personajes = "SELECT * FROM personajes"
        df_personajes = pd.read_sql_query(query_personajes, self.conn)
        
        # Obtener datos de actores de voz
        df_voice = None
        try:
            query_voice = """
            SELECT va.*, p.nombre as character_name
            FROM voice_actors va
            JOIN personajes p ON va.character_id = p.id
            """
            df_voice = pd.read_sql_query(query_voice, self.conn)
        except Exception as e:
            print(f"No se pudieron obtener datos de actores de voz: {e}")
        
        # Intentar guardar el archivo con manejo de errores
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Definir ruta del archivo, añadiendo timestamp si es reintento
                file_path = self.sample_path
                if attempt > 0:
                    # Añadir timestamp al nombre del archivo para evitar conflictos
                    base, ext = os.path.splitext(self.sample_path)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    file_path = f"{base}{ext}"
                
                # Crear un writer de Excel para guardar múltiples hojas
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    # Guardar datos de personajes
                    df_personajes.to_excel(writer, sheet_name='Personajes', index=False)
                    
                    # Guardar datos de actores de voz si existen
                    if df_voice is not None and not df_voice.empty:
                        df_voice.to_excel(writer, sheet_name='Actores de Voz', index=False)
                
                print(f"Archivo de muestra generado en {file_path}")
                return df_personajes
                
            except PermissionError:
                print(f"Error de permisos al escribir el archivo, intento {attempt+1}/{max_attempts}")
                if attempt == max_attempts - 1:
                    print("No se pudo escribir el archivo Excel, guardando como CSV en su lugar")
                    # Como alternativa, guardar como CSV
                    csv_path = self.sample_path.replace('.xlsx', '.csv')
                    df_personajes.to_csv(csv_path, index=False)
                    print(f"Datos guardados como CSV en {csv_path}")
            except Exception as e:
                print(f"Error al generar muestra: {e}")
                break
                
        return df_personajes
    
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
        
        # Contar actores de voz
        total_voice_actors_api = 0
        if 'data' in self.data:
            for personaje in self.data.get('data', []):
                total_voice_actors_api += len(personaje.get('voice_actors', []))
        
        try:
            query_voice = "SELECT COUNT(*) as total FROM voice_actors"
            df_voice = pd.read_sql_query(query_voice, self.conn)
            total_voice_actors_db = df_voice['total'].iloc[0]
        except:
            total_voice_actors_db = 0
        
        # Verificar si se agregaron duplicados
        if self.add_errors:
            registros_api_con_duplicados = registros_api + int(registros_api * 0.1)  # Aproximado
            print(f"Registros API con duplicados simulados: {registros_api_con_duplicados}")
        
        # Generar reporte
        with open(self.audit_path, 'w', encoding='utf-8') as f:
            f.write("=== REPORTE DE AUDITORÍA ===\n")
            f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write(f"Registros de personajes obtenidos del API: {registros_api}\n")
            if self.add_errors:
                f.write(f"Registros esperados con duplicados: aprox. {registros_api_con_duplicados}\n")
            f.write(f"Registros de personajes almacenados en la base de datos: {registros_db}\n\n")
            
            f.write(f"Registros de actores de voz obtenidos del API: {total_voice_actors_api}\n")
            f.write(f"Registros de actores de voz almacenados en la base de datos: {total_voice_actors_db}\n\n")
            
            if self.add_errors:
                f.write("NOTA: Se agregaron errores controlados a los datos para simular problemas de calidad.\n")
                f.write("Tipos de errores incluidos:\n")
                f.write("  - Valores nulos en campos aleatorios\n")
                f.write("  - Tipos de datos incorrectos (ej: texto en campos numéricos)\n")
                f.write("  - Registros duplicados con IDs diferentes\n")
                f.write("  - Caracteres especiales en campos de texto\n")
                f.write("  - Formatos de fecha inconsistentes\n")
                f.write("  - Valores extremos (outliers) en campos numéricos\n\n")
                
                f.write("Estos errores son intencionales para permitir realizar la actividad de limpieza de datos.\n\n")
            
            # Verificar diferencias
            diferencia = abs(registros_db - registros_api)
            if diferencia > 0 and not self.add_errors:
                f.write(f"⚠ ADVERTENCIA: Hay una diferencia de {diferencia} registros entre el API y la base de datos.\n")
            
            diferencia_va = abs(total_voice_actors_db - total_voice_actors_api)
            if diferencia_va > 0 and not self.add_errors:
                f.write(f"⚠ ADVERTENCIA: Hay una diferencia de {diferencia_va} registros de actores de voz entre el API y la base de datos.\n")
        
        print(f"Archivo de auditoría generado en {self.audit_path}")
    
    def procesar_datos_completo(self, params={}):
        """
        Ejecuta el proceso completo de ETL con manejo de errores.
        
        Args:
            params (dict): Parámetros para la solicitud a la API
            
        Returns:
            bool: True si el proceso fue exitoso, False en caso contrario
        """
        print("Iniciando proceso ETL...")
        resultado_final = True
        
        try:
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
            if self.add_errors:
                print("NOTA: Se agregarán errores controlados a los datos para simular problemas de calidad.")
                
            registros = self.insertar_datos()
            if registros == 0:
                print("ADVERTENCIA: No se insertaron registros en la base de datos.")
                resultado_final = False
            
            # Generación de evidencias
            try:
                print("Generando archivo de muestra...")
                self.generar_muestra()
            except Exception as e:
                print(f"ERROR al generar archivo de muestra: {e}")
                print("Continuando con el resto del proceso...")
                resultado_final = False
            
            try:
                print("Generando archivo de auditoría...")
                self.generar_auditoria()
            except Exception as e:
                print(f"ERROR al generar archivo de auditoría: {e}")
                resultado_final = False
            
            if resultado_final:
                print("Proceso ETL completado exitosamente.")
                if self.add_errors:
                    print("Se agregaron errores controlados a los datos para la actividad de limpieza.")
            else:
                print("Proceso ETL completado con advertencias.")
                
            return resultado_final
            
        except Exception as e:
            print(f"ERROR CRÍTICO durante el proceso ETL: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def mostrar_registros(self):
        """
        Muestra los registros almacenados en la base de datos.
        """
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
            
        print("\n=== PERSONAJES ===")
        query_personajes = "SELECT * FROM personajes"
        df_personajes = pd.read_sql_query(query_personajes, self.conn)
        
        if len(df_personajes) > 0:
            print(f"\nRegistros de personajes en la base de datos ({len(df_personajes)} total):")
            # Mostrar solo las primeras 20 filas para no saturar la consola
            print(df_personajes.head(20).to_string(index=False))
            if len(df_personajes) > 20:
                print(f"... mostrando 20 de {len(df_personajes)} registros")
        else:
            print("No hay registros de personajes en la base de datos.")
            
        try:
            print("\n=== ACTORES DE VOZ ===")
            query_voice = """
            SELECT va.id, va.person_id, va.person_name, va.language, 
                   p.nombre as character_name, va.fecha_extraccion
            FROM voice_actors va
            JOIN personajes p ON va.character_id = p.id
            LIMIT 20
            """
            df_voice = pd.read_sql_query(query_voice, self.conn)
            
            if len(df_voice) > 0:
                print(f"\nMuestra de actores de voz en la base de datos (mostrando 20 de {len(df_voice)} total):")
                print(df_voice.to_string(index=False))
            else:
                print("No hay registros de actores de voz en la base de datos.")
        except Exception as e:
            print(f"No se pudieron consultar los actores de voz: {e}")
        
        return df_personajes
    
    def cerrar_conexion(self):
        """
        Cierra la conexión a la base de datos.
        """
        if self.conn:
            self.conn.close()
            print("Conexión a la base de datos cerrada.")


# Ejecución del código cuando se llama directamente al script
if __name__ == "__main__":
    import sys
    
    # Parámetros para la API
    parametros = {"{id}": "20"}  # ID del anime Naruto por defecto
    
    # Control de errores (por defecto: agregar errores)
    add_errors = True
    error_rate = 0.3  # 30% de probabilidad de error por registro
    
    # Permitir especificar el ID del anime por línea de comandos
    if len(sys.argv) > 1:
        try:
            anime_id = int(sys.argv[1])
            parametros["{id}"] = str(anime_id)
            print(f"Usando ID de anime proporcionado: {anime_id}")
        except ValueError:
            print(f"ID de anime inválido: {sys.argv[1]}. Usando ID por defecto: 20")
    
    # Permitir especificar si se agregan errores
    if len(sys.argv) > 2:
        add_errors_arg = sys.argv[2].lower()
        if add_errors_arg in ('false', 'no', '0', 'n', 'f'):
            add_errors = False
            print("No se agregarán errores a los datos.")
        else:
            print("Se agregarán errores controlados a los datos.")
    
    # Permitir especificar la tasa de error
    if len(sys.argv) > 3:
        try:
            error_rate = float(sys.argv[3])
            if error_rate < 0 or error_rate > 1:
                print(f"Tasa de error debe estar entre 0.0 y 1.0. Usando valor por defecto: {error_rate}")
                error_rate = 0.3
            else:
                print(f"Usando tasa de error: {error_rate*100:.1f}%")
        except ValueError:
            print(f"Tasa de error inválida: {sys.argv[3]}. Usando valor por defecto: 30%")
    
    # Crear instancia del procesador
    procesador = AnimeDataProcessor(add_errors=add_errors, error_rate=error_rate)
    
    # Variable para controlar el resultado
    resultado = False
    
    try:
        # Ejecutar el proceso completo
        resultado = procesador.procesar_datos_completo(parametros)
        
        # Mostrar resumen solo si el proceso fue exitoso
        if resultado:
            procesador.mostrar_registros()
        else:
            print("\nNo se pudieron procesar todos los datos correctamente.")
            print("Revise los mensajes de error anteriores para más detalles.")
            
    except KeyboardInterrupt:
        print("\nProceso interrumpido por el usuario.")
    except Exception as e:
        print(f"\nERROR FATAL: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Asegurar que la conexión se cierre correctamente
        try:
            procesador.cerrar_conexion()
        except Exception as e:
            print(f"Error al cerrar la conexión: {e}")