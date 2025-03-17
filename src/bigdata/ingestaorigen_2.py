"""
Actividad 2: Carga de Datos en la Nube (Simulada), Análisis y Limpieza
Este script simula la carga de datos desde una base de datos local a un entorno de nube,
realiza análisis exploratorio y procesos de limpieza/transformación utilizando Pandas.
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import logging
import re
from datetime import datetime

class DataCloudProcessor:
    """Clase para simular la carga, limpieza y transformación de datos desde la nube"""
    
    def __init__(self, input_db_path='src/bigdata/static/db/anime_data.db'):
        """
        Inicializa el procesador de datos en la nube simulada.
        
        Args:
            input_db_path (str): Ruta a la base de datos SQLite de entrada
        """
        self.input_db_path = input_db_path
        self.cleaned_data_path = 'src/bigdata/static/db/cleaned_data.xlsx'
        self.csv_cleaned_path = 'src/bigdata/static/db/cleaned_data.csv'
        self.report_path = 'src/bigdata/static/auditoria/cleaning_report.txt'
        self.conn = None
        self.raw_data = None
        self.cleaned_df = None
        
        # Crear estructura de directorios si no existe
        os.makedirs(os.path.dirname(self.input_db_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.report_path), exist_ok=True)
        
        # Configurar logging
        self._setup_logging()
    
    def _setup_logging(self):
        """Configura el sistema de logging para el reporte de auditoría"""
        logging.basicConfig(
            filename=self.report_path,
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filemode='w'  # Sobrescribir archivo existente
        )
        self.logger = logging.getLogger()
        
        # Añadir también log a consola
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        console.setFormatter(formatter)
        self.logger.addHandler(console)
        
        self.logger.info("=== REPORTE DE LIMPIEZA Y TRANSFORMACIÓN DE DATOS ===")
        self.logger.info(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("="*60)
    
    def connect_to_source(self):
        """Conecta a la base de datos SQLite que contiene los datos de la Actividad 1"""
        try:
            if not os.path.exists(self.input_db_path):
                self.logger.error(f"La base de datos {self.input_db_path} no existe.")
                return False
                
            self.conn = sqlite3.connect(self.input_db_path)
            self.logger.info(f"Conexión exitosa a la base de datos: {self.input_db_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error al conectar a la base de datos: {str(e)}")
            return False
    
    def load_data(self):
        """Carga los datos desde la base de datos SQLite a un DataFrame de Pandas"""
        if not self.conn:
            if not self.connect_to_source():
                return False
        
        try:
            # Obtener lista de tablas en la base de datos
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            if not tables:
                self.logger.error("No se encontraron tablas en la base de datos.")
                return False
            
            # Usar la primera tabla disponible
            table_name = tables[0][0]
            self.logger.info(f"Cargando datos de la tabla: {table_name}")
            
            # Cargar datos a pandas DataFrame
            self.raw_data = pd.read_sql_query(f"SELECT * FROM {table_name}", self.conn)
            
            if len(self.raw_data) == 0:
                self.logger.warning("La tabla está vacía. Simulando datos para demostración.")
                self._simulate_data()
            elif len(self.raw_data) < 5:
                self.logger.info(f"Solo se encontraron {len(self.raw_data)} registros. Añadiendo datos simulados para mejor demostración.")
                self._enrich_data()
            
            record_count = len(self.raw_data)
            self.logger.info(f"Datos cargados exitosamente: {record_count} registros.")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error al cargar datos: {str(e)}")
            return False
    
    def _simulate_data(self):
        """Crea datos simulados si no hay datos en la base de datos"""
        # Crear un conjunto de datos simulado básico para anime
        self.raw_data = pd.DataFrame({
            'id': range(1, 21),
            'nombre': [f"Personaje {i}" for i in range(1, 21)],
            'nombre_kanji': [None if i % 3 == 0 else f"キャラクター {i}" for i in range(1, 21)],
            'role': ["Protagonista", "Antagonista", "Secundario", None] * 5,
            'anime_id': [i // 2 for i in range(1, 21)],
            'imagen_url': [f"https://example.com/img/{i}.jpg" if i % 4 != 0 else None for i in range(1, 21)],
            'fecha_extraccion': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 20
        })
        
        # Introducir errores para limpieza
        # 1. Duplicar algunos registros
        duplicates = self.raw_data.iloc[0:3].copy()
        
        # 2. Cambiar tipos en algunos registros
        type_errors = self.raw_data.iloc[5:8].copy()
        type_errors['id'] = type_errors['id'].astype(str) + 'X'
        type_errors['anime_id'] = type_errors['anime_id'].astype(str) + 'Y'
        
        # 3. Introducir outliers
        outliers = self.raw_data.iloc[10:12].copy()
        outliers['anime_id'] = 9999
        
        # 4. Añadir caracteres especiales
        special_chars = self.raw_data.iloc[15:18].copy()
        special_chars['nombre'] = special_chars['nombre'] + '@#$%'
        
        # Combinar todos los datos
        self.raw_data = pd.concat([self.raw_data, duplicates, type_errors, outliers, special_chars], ignore_index=True)
        
        self.logger.info(f"Se generaron {len(self.raw_data)} registros simulados con problemas de calidad para demostración.")
    
    def _enrich_data(self):
        """Enriquece los datos existentes con registros simulados adicionales"""
        existing_data = self.raw_data.copy()
        
        # Crear registros adicionales basados en los existentes
        additional_data = []
        
        # 1. Duplicar registros existentes
        for _, row in existing_data.iterrows():
            duplicate = row.copy()
            additional_data.append(duplicate)
        
        # 2. Crear registros con valores nulos
        for _, row in existing_data.iterrows():
            null_record = row.copy()
            for col in null_record.index:
                if col not in ['id', 'anime_id'] and np.random.random() > 0.7:
                    null_record[col] = None
            additional_data.append(null_record)
        
        # 3. Crear registros con tipos inconsistentes
        for _, row in existing_data.iterrows():
            inconsistent_record = row.copy()
            if 'id' in inconsistent_record and inconsistent_record['id'] is not None:
                inconsistent_record['id'] = f"ID-{inconsistent_record['id']}"
            if 'anime_id' in inconsistent_record and inconsistent_record['anime_id'] is not None:
                inconsistent_record['anime_id'] = f"A{inconsistent_record['anime_id']}"
            additional_data.append(inconsistent_record)
        
        # 4. Crear outliers
        for _, row in existing_data.iterrows():
            outlier_record = row.copy()
            if 'anime_id' in outlier_record and outlier_record['anime_id'] is not None:
                try:
                    outlier_record['anime_id'] = 99999
                except:
                    pass
            additional_data.append(outlier_record)
        
        # Convertir a DataFrame y combinar con datos originales
        additional_df = pd.DataFrame(additional_data)
        self.raw_data = pd.concat([self.raw_data, additional_df], ignore_index=True)
        
        self.logger.info(f"Datos enriquecidos: {len(self.raw_data)} registros totales con problemas de calidad simulados.")
    
    def exploratory_analysis(self):
        """Realiza análisis exploratorio para identificar problemas de calidad"""
        if self.raw_data is None:
            self.logger.error("No hay datos para analizar.")
            return False
        
        self.logger.info("\n" + "="*60)
        self.logger.info("ANÁLISIS EXPLORATORIO DE DATOS")
        self.logger.info("="*60)
        
        # 1. Estadísticas básicas
        total_records = len(self.raw_data)
        self.logger.info(f"Total de registros: {total_records}")
        
        # 2. Contar duplicados
        distinct_records = len(self.raw_data.drop_duplicates())
        duplicates = total_records - distinct_records
        self.logger.info(f"Registros duplicados: {duplicates}")
        
        # 3. Análisis de valores nulos por columna
        self.logger.info("\nValores nulos por columna:")
        null_counts = self.raw_data.isna().sum()
        null_counts = null_counts[null_counts > 0]
        
        if not null_counts.empty:
            for column, count in null_counts.items():
                self.logger.info(f"  • {column}: {count} valores nulos")
        else:
            self.logger.info("  • No se encontraron valores nulos")
        
        # 4. Verificar problemas de tipo de datos
        self.logger.info("\nProblemas potenciales de tipo de datos:")
        type_issues = 0
        
        # Verificar columnas que deberían ser numéricas
        for col_name in ['id', 'anime_id']:
            if col_name in self.raw_data.columns:
                # Filtrar valores que no son nulos pero no se pueden convertir a enteros
                invalid_count = 0
                for value in self.raw_data[col_name].dropna():
                    try:
                        int(value)
                    except (ValueError, TypeError):
                        invalid_count += 1
                
                if invalid_count > 0:
                    type_issues += invalid_count
                    self.logger.info(f"  • {col_name}: {invalid_count} valores no numéricos")
        
        if type_issues == 0:
            self.logger.info("  • No se detectaron problemas de tipo de datos")
        
        # 5. Identificar posibles outliers en columnas numéricas
        self.logger.info("\nPosibles outliers:")
        outliers_detected = False
        
        for col_name in ['id', 'anime_id']:
            if col_name in self.raw_data.columns:
                # Intentar convertir a numérico (ignorando errores)
                numeric_values = pd.to_numeric(self.raw_data[col_name], errors='coerce')
                numeric_values = numeric_values.dropna()
                
                if len(numeric_values) > 0:
                    # Calcular estadísticas
                    q1 = numeric_values.quantile(0.25)
                    q3 = numeric_values.quantile(0.75)
                    iqr = q3 - q1
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    
                    outliers = numeric_values[(numeric_values < lower_bound) | (numeric_values > upper_bound)]
                    
                    if len(outliers) > 0:
                        outliers_detected = True
                        self.logger.info(f"  • {col_name}: {len(outliers)} posibles outliers")
        
        if not outliers_detected:
            self.logger.info("  • No se detectaron outliers significativos")
        
        # 6. Resumen estadístico
        self.logger.info("\nResumen estadístico de columnas:")
        desc_stats = self.raw_data.describe(include='all')
        
        for column in self.raw_data.columns:
            if column in desc_stats:
                try:
                    min_val = desc_stats[column]['min'] if 'min' in desc_stats[column] else 'N/A'
                    max_val = desc_stats[column]['max'] if 'max' in desc_stats[column] else 'N/A'
                    self.logger.info(f"  • {column}: min={min_val}, max={max_val}")
                except:
                    self.logger.info(f"  • {column}: estadísticas no disponibles")
        
        self.logger.info("="*60)
        return True
    
    def clean_and_transform(self):
        """Realiza la limpieza y transformación de los datos"""
        if self.raw_data is None:
            self.logger.error("No hay datos para limpiar.")
            return False
        
        self.logger.info("\n" + "="*60)
        self.logger.info("LIMPIEZA Y TRANSFORMACIÓN DE DATOS")
        self.logger.info("="*60)
        
        initial_count = len(self.raw_data)
        self.logger.info(f"Registros iniciales: {initial_count}")
        
        # 1. Eliminar duplicados
        df_no_duplicates = self.raw_data.drop_duplicates()
        duplicates_removed = initial_count - len(df_no_duplicates)
        self.logger.info(f"\n1. Eliminación de duplicados:")
        self.logger.info(f"   • {duplicates_removed} registros duplicados eliminados")
        
        # 2. Manejo de valores nulos
        self.logger.info("\n2. Manejo de valores nulos:")
        
        # Imputar valores para cada columna según su naturaleza
        df_no_nulls = df_no_duplicates.copy()
        
        # Lista para almacenar operaciones realizadas
        null_operations = []
        
        # Para cada columna, aplicar estrategia de imputación específica
        for col_name in df_no_duplicates.columns:
            null_count = df_no_duplicates[col_name].isna().sum()
            
            if null_count > 0:
                if col_name == 'nombre':
                    df_no_nulls[col_name] = df_no_nulls[col_name].fillna("Nombre Desconocido")
                    null_operations.append(f"Imputación de {null_count} valores nulos en '{col_name}' con 'Nombre Desconocido'")
                    
                elif col_name == 'nombre_kanji':
                    df_no_nulls[col_name] = df_no_nulls[col_name].fillna("")
                    null_operations.append(f"Imputación de {null_count} valores nulos en '{col_name}' con cadena vacía")
                    
                elif col_name == 'role':
                    df_no_nulls[col_name] = df_no_nulls[col_name].fillna("DESCONOCIDO")
                    null_operations.append(f"Imputación de {null_count} valores nulos en '{col_name}' con 'DESCONOCIDO'")
                    
                elif col_name == 'imagen_url':
                    df_no_nulls[col_name] = df_no_nulls[col_name].fillna("https://placeholder.com/no_image.jpg")
                    null_operations.append(f"Imputación de {null_count} valores nulos en '{col_name}' con URL de imagen por defecto")
                    
                elif col_name == 'fecha_extraccion':
                    df_no_nulls[col_name] = df_no_nulls[col_name].fillna(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    null_operations.append(f"Imputación de {null_count} valores nulos en '{col_name}' con fecha actual")
        
        # Registrar operaciones realizadas
        if null_operations:
            for operation in null_operations:
                self.logger.info(f"   • {operation}")
        else:
            self.logger.info("   • No se encontraron valores nulos para imputar")
        
        # 3. Corrección de tipos de datos
        self.logger.info("\n3. Corrección de tipos de datos:")
        
        # Lista para almacenar operaciones realizadas
        type_operations = []
        
        # Corrección de tipos por columna
        df_corrected = df_no_nulls.copy()
        
        # Corregir tipo de id
        if 'id' in df_corrected.columns:
            invalid_ids = 0
            
            # Función para convertir a entero o usar un valor predeterminado (0)
            def convert_to_int(value):
                nonlocal invalid_ids
                try:
                    return int(value)
                except (ValueError, TypeError):
                    invalid_ids += 1
                    return 0
            
            df_corrected['id'] = df_corrected['id'].apply(convert_to_int)
            
            if invalid_ids > 0:
                type_operations.append(f"Corrección de {invalid_ids} valores no numéricos en 'id'")
        
        # Corregir tipo de anime_id
        if 'anime_id' in df_corrected.columns:
            invalid_anime_ids = 0
            
            def convert_to_int(value):
                nonlocal invalid_anime_ids
                try:
                    return int(value)
                except (ValueError, TypeError):
                    invalid_anime_ids += 1
                    return 0
            
            df_corrected['anime_id'] = df_corrected['anime_id'].apply(convert_to_int)
            
            if invalid_anime_ids > 0:
                type_operations.append(f"Corrección de {invalid_anime_ids} valores no numéricos en 'anime_id'")
        
        # Registrar operaciones realizadas
        if type_operations:
            for operation in type_operations:
                self.logger.info(f"   • {operation}")
        else:
            self.logger.info("   • No se encontraron problemas de tipo de datos para corregir")
        
        # 4. Transformaciones adicionales
        self.logger.info("\n4. Transformaciones adicionales:")
        
        # 4.1 Normalizar nombres (eliminar caracteres especiales)
        if 'nombre' in df_corrected.columns:
            # Función para eliminar caracteres especiales
            def clean_name(name):
                if pd.isna(name):
                    return name
                return re.sub(r'[^a-zA-Z0-9\s]', '', str(name))
            
            df_corrected['nombre'] = df_corrected['nombre'].apply(clean_name)
            self.logger.info("   • Normalización de nombres: eliminación de caracteres especiales")
        
        # 4.2 Convertir 'role' a mayúsculas para consistencia
        if 'role' in df_corrected.columns:
            # Función para convertir a mayúsculas
            def to_upper(value):
                if pd.isna(value):
                    return value
                return str(value).upper()
            
            df_corrected['role'] = df_corrected['role'].apply(to_upper)
            self.logger.info("   • Estandarización de roles: conversión a mayúsculas")
        
        # Guardar el resultado de la limpieza
        self.cleaned_df = df_corrected
        
        # Contar registros finales
        final_count = len(self.cleaned_df)
        records_removed = initial_count - final_count
        
        self.logger.info(f"\nResumen del proceso de limpieza:")
        self.logger.info(f"   • Registros iniciales: {initial_count}")
        self.logger.info(f"   • Registros finales: {final_count}")
        self.logger.info(f"   • Registros eliminados: {records_removed}")
        
        self.logger.info("="*60)
        return True
    
    def generate_evidence(self):
        """Genera archivos de evidencia del proceso de limpieza"""
        if self.cleaned_df is None:
            self.logger.error("No hay datos limpios para generar evidencias.")
            return False
        
        self.logger.info("\n" + "="*60)
        self.logger.info("GENERACIÓN DE EVIDENCIAS")
        self.logger.info("="*60)
        
        try:
            # Crear directorios si no existen
            os.makedirs(os.path.dirname(self.cleaned_data_path), exist_ok=True)
            
            # Guardar en Excel
            self.cleaned_df.to_excel(self.cleaned_data_path, index=False)
            self.logger.info(f"• Datos limpios guardados en Excel: {self.cleaned_data_path}")
            
            # Guardar en CSV
            self.cleaned_df.to_csv(self.csv_cleaned_path, index=False)
            self.logger.info(f"• Datos limpios guardados en CSV: {self.csv_cleaned_path}")
            
            # El archivo de auditoría ya se está generando a través del logger
            self.logger.info(f"• Reporte de auditoría generado en: {self.report_path}")
            
            # Añadir resumen final al reporte
            self.logger.info("\nRESUMEN FINAL DEL PROCESO")
            self.logger.info("="*60)
            
            original_count = len(self.raw_data) if self.raw_data is not None else 0
            final_count = len(self.cleaned_df)
            
            self.logger.info(f"• Registros iniciales: {original_count}")
            self.logger.info(f"• Registros finales después de limpieza: {final_count}")
            self.logger.info(f"• Diferencia: {original_count - final_count} registros")
            self.logger.info(f"• Operaciones realizadas:")
            self.logger.info(f"  - Eliminación de duplicados")
            self.logger.info(f"  - Imputación de valores nulos")
            self.logger.info(f"  - Corrección de tipos de datos")
            self.logger.info(f"  - Normalización de textos")
            
            self.logger.info("="*60)
            self.logger.info("PROCESO COMPLETADO EXITOSAMENTE")
            self.logger.info("="*60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error al generar evidencias: {str(e)}")
            return False
    
    def process_data(self):
        """Ejecuta el proceso completo de carga, limpieza y generación de evidencias"""
        self.logger.info("Iniciando procesamiento de datos en la nube (simulado)...")
        
        # 1. Cargar datos
        if not self.load_data():
            self.logger.error("El proceso se detuvo debido a errores en la carga de datos.")
            return False
        
        # 2. Análisis exploratorio
        if not self.exploratory_analysis():
            self.logger.error("El proceso se detuvo debido a errores en el análisis exploratorio.")
            return False
        
        # 3. Limpieza y transformación
        if not self.clean_and_transform():
            self.logger.error("El proceso se detuvo debido a errores en la limpieza de datos.")
            return False
        
        # 4. Generación de evidencias
        if not self.generate_evidence():
            self.logger.error("El proceso se detuvo debido a errores en la generación de evidencias.")
            return False
        
        self.logger.info("Proceso de datos en la nube (simulado) completado exitosamente.")
        return True
    
    def close(self):
        """Cierra conexiones y libera recursos"""
        if self.conn:
            self.conn.close()
            self.logger.info("Conexión a la base de datos cerrada.")

# Ejecución del código cuando se llama directamente al script
if __name__ == "__main__":
    processor = DataCloudProcessor()
    
    try:
        success = processor.process_data()
        if success:
            print("Proceso completado exitosamente. Revisa los archivos de evidencia generados.")
        else:
            print("El proceso se detuvo con errores. Consulta el archivo de log para más detalles.")
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
    finally:
        processor.close()