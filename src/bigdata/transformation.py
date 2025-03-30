"""
Actividad 3: Enriquecimiento de Datos
Este script realiza la integración y enriquecimiento de datos a partir de fuentes diversas.
"""

import pandas as pd
import numpy as np
import os
import json
import logging
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
import re

class DataEnrichmentProcessor:
    """Clase para el enriquecimiento de datos desde múltiples fuentes"""
    
    def __init__(self):
        """
        Inicializa el procesador de enriquecimiento de datos
        """
        # Definir rutas para archivos correctas según la estructura existente
        self.base_dir = Path('src/bigdata')
        self.static_dir = self.base_dir / 'static'
        self.db_dir = self.static_dir / 'db'
        self.audit_dir = self.static_dir / 'auditoria'
        self.xlsx_dir = self.static_dir / 'db'  # Para mantener el Excel dentro de db
        
        # Rutas de datos de entrada (dataset base de actividad 2)
        self.base_dataset_path = 'src/bigdata/static/db/cleaned_data.csv'
        
        # Rutas de salida
        self.enriched_data_path = 'src/bigdata/static/db/enriched_data.xlsx'
        self.enriched_csv_path = 'src/bigdata/static/db/enriched_data.csv'
        self.audit_path = 'src/bigdata/static/auditoria/enriched_report.txt'
        
        # Crear estructura de directorios si no existe
        self.create_directory_structure()
        
        # Inicializar dataframes
        self.base_df = None
        self.part1_df = None
        self.part2_df = None
        self.json_df = None
        self.xlsx_df = None
        self.enriched_df = None
        
        # Configurar logging
        self._setup_logging()
    
    def create_directory_structure(self):
        """Crea la estructura de directorios necesaria"""
        for directory in [self.db_dir, self.audit_dir, self.xlsx_dir]:
            os.makedirs(directory, exist_ok=True)
    
    def _setup_logging(self):
        """Configura el sistema de logging para el reporte de auditoría"""
        logging.basicConfig(
            filename=self.audit_path,
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
        
        self.logger.info("=== REPORTE DE ENRIQUECIMIENTO DE DATOS ===")
        self.logger.info(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("="*60)
    
    def load_base_dataset(self):
        """Carga el dataset base (limpio) de la Actividad 2"""
        try:
            self.logger.info(f"Cargando dataset base desde: {self.base_dataset_path}")
            
            if not os.path.exists(self.base_dataset_path):
                self.logger.error(f"El archivo {self.base_dataset_path} no existe.")
                # Simular datos si no existe el dataset base
                self.logger.info("Generando datos simulados para demostración...")
                self._simulate_base_data()
                return True
            
            # Cargar el dataset base
            self.base_df = pd.read_csv(self.base_dataset_path)
            self.logger.info(f"Dataset base cargado exitosamente: {len(self.base_df)} registros")
            
            return True
        except Exception as e:
            self.logger.error(f"Error al cargar el dataset base: {str(e)}")
            # Simular datos en caso de error
            self.logger.info("Generando datos simulados para demostración...")
            self._simulate_base_data()
            return False
    
    def _simulate_base_data(self):
        """Crea datos simulados si no se puede cargar el dataset base"""
        # Crear un DataFrame simulado para anime
        self.base_df = pd.DataFrame({
            'id': range(1, 21),
            'nombre': [f"Personaje {i}" for i in range(1, 21)],
            'nombre_kanji': [f"キャラクター {i}" for i in range(1, 21)],
            'role': ["PROTAGONISTA", "ANTAGONISTA", "SECUNDARIO"] * 7,
            'anime_id': [i // 2 for i in range(1, 21)],
            'imagen_url': [f"https://example.com/img/{i}.jpg" for i in range(1, 21)],
            'fecha_extraccion': [datetime.now().strftime("%Y-%m-%d")] * 20
        })
        
        self.logger.info(f"Se generaron {len(self.base_df)} registros simulados para el dataset base")
    
    def split_base_dataset(self):
        """Divide el dataset base en dos partes para simular fuentes distintas"""
        if self.base_df is None:
            self.logger.error("No hay dataset base para dividir")
            return False
        
        self.logger.info("Dividiendo el dataset base en dos partes...")
        
        # Obtener la mitad de las columnas para cada parte
        all_columns = list(self.base_df.columns)
        common_columns = ['id', 'nombre']  # Columnas comunes para poder hacer join
        
        # Definir columnas específicas para cada parte
        part1_columns = common_columns + ['anime_id', 'role']
        remaining_columns = [col for col in all_columns if col not in part1_columns]
        part2_columns = common_columns + remaining_columns
        
        # Crear las dos partes
        self.part1_df = self.base_df[part1_columns].copy()
        self.part2_df = self.base_df[part2_columns].copy()
        
        # Guardar temporalmente como CSV y JSON para simular fuentes adicionales
        part1_csv_path = 'src/bigdata/static/db/anime_part1.csv'
        part2_json_path = 'src/bigdata/static/db/anime_part2.json'
        
        self.part1_df.to_csv(part1_csv_path, index=False)
        self.part2_df.to_json(part2_json_path, orient='records')
        
        self.logger.info(f"Parte 1 (CSV) creada con {len(self.part1_df)} registros y columnas: {', '.join(part1_columns)}")
        self.logger.info(f"Parte 2 (JSON) creada con {len(self.part2_df)} registros y columnas: {', '.join(part2_columns)}")
        
        # Generar también un Excel con algunos datos adicionales (simulando otra fuente)
        self._create_additional_excel()
        
        # Crear XML con información complementaria
        self._create_additional_xml()
        
        return True
    
    def _create_additional_excel(self):
        """Crea un archivo Excel con información adicional"""
        # Crear datos complementarios (ej: popularidad, ranking)
        self.xlsx_df = pd.DataFrame({
            'id': self.base_df['id'].tolist(),
            'popularidad': np.random.randint(1, 100, size=len(self.base_df)),
            'ranking': np.random.randint(1, 500, size=len(self.base_df)),
            'votos': np.random.randint(100, 10000, size=len(self.base_df))
        })
        
        # Guardar como Excel
        xlsx_path = 'src/bigdata/static/db/anime_extra.xlsx'
        self.xlsx_df.to_excel(xlsx_path, index=False)
        
        self.logger.info(f"Archivo Excel adicional creado con {len(self.xlsx_df)} registros")
    
    def _create_additional_xml(self):
        """Crea un archivo XML con información complementaria"""
        # Crear estructura XML para categorías
        root = ET.Element("categorias")
        
        # Tomar una muestra de IDs para asignar categorías
        sample_ids = self.base_df['id'].sample(n=min(10, len(self.base_df))).tolist()
        
        categories = ["Acción", "Aventura", "Comedia", "Drama", "Fantasía", "Sci-Fi", "Romance"]
        
        for idx, id_value in enumerate(sample_ids):
            anime = ET.SubElement(root, "anime")
            id_elem = ET.SubElement(anime, "id")
            id_elem.text = str(id_value)
            
            # Asignar 2-3 categorías aleatorias
            num_categories = np.random.randint(2, 4)
            selected_categories = np.random.choice(categories, num_categories, replace=False)
            
            for category in selected_categories:
                cat_elem = ET.SubElement(anime, "categoria")
                cat_elem.text = category
        
        # Guardar como XML
        xml_path = 'src/bigdata/static/db/anime_categories.xml'
        tree = ET.ElementTree(root)
        tree.write(xml_path, encoding='utf-8', xml_declaration=True)
        
        self.logger.info(f"Archivo XML adicional creado con categorías para {len(sample_ids)} animes")
    
    def read_additional_sources(self):
        """Lee las fuentes adicionales en diferentes formatos"""
        self.logger.info("\n" + "="*60)
        self.logger.info("LECTURA DE FUENTES ADICIONALES")
        self.logger.info("="*60)
        
        # Variables para almacenar dataframes de fuentes adicionales
        sources_data = {}
        
        try:
            # 1. Leer CSV
            csv_path = 'src/bigdata/static/db/anime_part1.csv'
            if os.path.exists(csv_path):
                sources_data['csv'] = pd.read_csv(csv_path)
                self.logger.info(f"CSV leído correctamente: {len(sources_data['csv'])} registros")
            else:
                self.logger.warning(f"Archivo CSV no encontrado: {csv_path}")
            
            # 2. Leer JSON
            json_path = 'src/bigdata/static/db/anime_part2.json'
            if os.path.exists(json_path):
                with open(json_path, 'r') as f:
                    json_data = json.load(f)
                sources_data['json'] = pd.DataFrame(json_data)
                self.logger.info(f"JSON leído correctamente: {len(sources_data['json'])} registros")
            else:
                self.logger.warning(f"Archivo JSON no encontrado: {json_path}")
            
            # 3. Leer Excel
            excel_path = 'src/bigdata/static/db/anime_extra.xlsx'
            if os.path.exists(excel_path):
                sources_data['excel'] = pd.read_excel(excel_path)
                self.logger.info(f"Excel leído correctamente: {len(sources_data['excel'])} registros")
            else:
                self.logger.warning(f"Archivo Excel no encontrado: {excel_path}")
            
            # 4. Leer XML
            xml_path = 'src/bigdata/static/db/anime_categories.xml'
            if os.path.exists(xml_path):
                # Procesar XML para extraer categorías
                categories_data = []
                tree = ET.parse(xml_path)
                root = tree.getroot()
                
                for anime in root.findall('anime'):
                    anime_id = anime.find('id').text
                    categories = [cat.text for cat in anime.findall('categoria')]
                    categories_data.append({
                        'id': int(anime_id),
                        'categorias': ', '.join(categories)
                    })
                
                sources_data['xml'] = pd.DataFrame(categories_data)
                self.logger.info(f"XML leído correctamente: {len(sources_data['xml'])} registros")
            else:
                self.logger.warning(f"Archivo XML no encontrado: {xml_path}")
            
            # Guardar los dataframes para usar en la integración
            self.additional_sources = sources_data
            
            self.logger.info(f"Total de fuentes adicionales leídas: {len(sources_data)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al leer fuentes adicionales: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def integrate_data(self):
        """Integra las diferentes fuentes de datos"""
        if not hasattr(self, 'additional_sources') or not self.additional_sources:
            self.logger.error("No hay fuentes adicionales para integrar")
            return False
        
        self.logger.info("\n" + "="*60)
        self.logger.info("INTEGRACIÓN DE DATOS")
        self.logger.info("="*60)
        
        try:
            # Empezar con el dataset base
            if self.base_df is not None:
                result_df = self.base_df.copy()
                self.logger.info(f"Usando dataset base como inicio: {len(result_df)} registros")
            else:
                # Si no hay dataset base, empezar con la fuente CSV
                if 'csv' in self.additional_sources:
                    result_df = self.additional_sources['csv'].copy()
                    self.logger.info(f"Usando fuente CSV como inicio: {len(result_df)} registros")
                else:
                    self.logger.error("No se puede integrar: falta el dataset base y la fuente CSV")
                    return False
            
            # Integrar con JSON (join por 'id')
            if 'json' in self.additional_sources:
                json_df = self.additional_sources['json']
                
                # Identificar columnas a agregar (excepto las que ya existen)
                existing_cols = set(result_df.columns)
                json_cols = [col for col in json_df.columns if col not in existing_cols or col == 'id']
                
                # Merge con JSON
                result_df = pd.merge(
                    result_df, 
                    json_df[json_cols], 
                    on='id', 
                    how='left',
                    suffixes=('', '_json')
                )
                
                # Registrar la operación
                self.logger.info(f"Integración con JSON: {len(json_df)} registros, {len(json_cols)} columnas")
                self.logger.info(f"Columnas agregadas de JSON: {', '.join([col for col in json_cols if col != 'id'])}")
            
            # Integrar con Excel (join por 'id')
            if 'excel' in self.additional_sources:
                excel_df = self.additional_sources['excel']
                
                # Identificar columnas a agregar (excepto las que ya existen)
                existing_cols = set(result_df.columns)
                excel_cols = [col for col in excel_df.columns if col not in existing_cols or col == 'id']
                
                # Merge con Excel
                result_df = pd.merge(
                    result_df, 
                    excel_df[excel_cols], 
                    on='id', 
                    how='left',
                    suffixes=('', '_excel')
                )
                
                # Registrar la operación
                self.logger.info(f"Integración con Excel: {len(excel_df)} registros, {len(excel_cols)} columnas")
                self.logger.info(f"Columnas agregadas de Excel: {', '.join([col for col in excel_cols if col != 'id'])}")
            
            # Integrar con XML (join por 'id')
            if 'xml' in self.additional_sources:
                xml_df = self.additional_sources['xml']
                
                # Merge con XML
                result_df = pd.merge(
                    result_df, 
                    xml_df, 
                    on='id', 
                    how='left',
                    suffixes=('', '_xml')
                )
                
                # Rellenar valores nulos en la columna de categorías
                if 'categorias' in result_df.columns:
                    result_df['categorias'] = result_df['categorias'].fillna('Sin categoría')
                
                # Registrar la operación
                self.logger.info(f"Integración con XML: {len(xml_df)} registros, 1 columna")
                self.logger.info(f"Columna agregada de XML: categorias")
            
            # Almacenar el resultado
            self.enriched_df = result_df
            
            # Contabilizar mejoras
            initial_columns = len(self.base_df.columns) if self.base_df is not None else 0
            final_columns = len(self.enriched_df.columns)
            new_columns = final_columns - initial_columns
            
            self.logger.info(f"Resultado de la integración:")
            self.logger.info(f"- Registros en dataset enriquecido: {len(self.enriched_df)}")
            self.logger.info(f"- Columnas originales: {initial_columns}")
            self.logger.info(f"- Columnas nuevas: {new_columns}")
            self.logger.info(f"- Total columnas: {final_columns}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error al integrar datos: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def generate_evidence(self):
        """Genera los archivos de evidencia del proceso de enriquecimiento"""
        if self.enriched_df is None:
            self.logger.error("No hay datos enriquecidos para generar evidencias")
            return False
        
        self.logger.info("\n" + "="*60)
        self.logger.info("GENERACIÓN DE EVIDENCIAS")
        self.logger.info("="*60)
        
        try:
            # Guardar en Excel
            self.enriched_df.to_excel(self.enriched_data_path, index=False)
            self.logger.info(f"Datos enriquecidos guardados en Excel: {self.enriched_data_path}")
            
            # Guardar en CSV
            self.enriched_df.to_csv(self.enriched_csv_path, index=False)
            self.logger.info(f"Datos enriquecidos guardados en CSV: {self.enriched_csv_path}")
            
            # Añadir resumen al archivo de auditoría
            self.logger.info("\nRESUMEN FINAL DEL PROCESO DE ENRIQUECIMIENTO")
            self.logger.info("="*60)
            
            # Contabilizar métricas principales
            base_records = len(self.base_df) if self.base_df is not None else 0
            enriched_records = len(self.enriched_df)
            
            base_columns = len(self.base_df.columns) if self.base_df is not None else 0
            enriched_columns = len(self.enriched_df.columns)
            
            # Mostrar resumen en el log
            self.logger.info(f"• Dataset Base:")
            self.logger.info(f"  - Registros: {base_records}")
            self.logger.info(f"  - Columnas: {base_columns}")
            
            self.logger.info(f"\n• Dataset Enriquecido:")
            self.logger.info(f"  - Registros: {enriched_records}")
            self.logger.info(f"  - Columnas: {enriched_columns}")
            self.logger.info(f"  - Nuevas columnas añadidas: {enriched_columns - base_columns}")
            
            # Información sobre el join/merge
            self.logger.info(f"\n• Operaciones de cruce:")
            for source in self.additional_sources:
                source_records = len(self.additional_sources[source])
                self.logger.info(f"  - Cruce con fuente {source.upper()}: {source_records} registros evaluados")
            
            # Estadísticas adicionales
            null_count = self.enriched_df.isnull().sum().sum()
            self.logger.info(f"\n• Estadísticas de calidad:")
            self.logger.info(f"  - Valores nulos en dataset enriquecido: {null_count}")
            self.logger.info(f"  - Porcentaje de completitud: {100 - (null_count / (enriched_records * enriched_columns) * 100):.2f}%")
            
            self.logger.info("\n• Análisis de columnas enriquecidas:")
            for col in self.enriched_df.columns:
                if col not in self.base_df.columns and self.base_df is not None:
                    null_percentage = (self.enriched_df[col].isnull().sum() / len(self.enriched_df)) * 100
                    self.logger.info(f"  - {col}: {100 - null_percentage:.2f}% de datos completos")
            
            self.logger.info("="*60)
            self.logger.info("PROCESO DE ENRIQUECIMIENTO COMPLETADO EXITOSAMENTE")
            self.logger.info("="*60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error al generar evidencias: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def run_enrichment_process(self):
        """Ejecuta el proceso completo de enriquecimiento"""
        self.logger.info("Iniciando proceso de enriquecimiento de datos...")
        
        # Cargar dataset base
        if not self.load_base_dataset():
            self.logger.warning("Se detectaron problemas al cargar el dataset base. Continuando con datos simulados.")
        
        # Dividir dataset base (simulando fuentes distintas)
        if not self.split_base_dataset():
            self.logger.error("Error al dividir el dataset base. El proceso no puede continuar.")
            return False
        
        # Leer fuentes adicionales
        if not self.read_additional_sources():
            self.logger.error("Error al leer fuentes adicionales. El proceso no puede continuar.")
            return False
        
        # Integrar datos
        if not self.integrate_data():
            self.logger.error("Error al integrar datos. El proceso no puede continuar.")
            return False
        
        # Generar evidencias
        if not self.generate_evidence():
            self.logger.error("Error al generar evidencias.")
            return False
        
        self.logger.info("Proceso de enriquecimiento completado exitosamente.")
        return True


# Ejecución del código cuando se llama directamente al script
if __name__ == "__main__":
    enrichment = DataEnrichmentProcessor()
    
    try:
        success = enrichment.run_enrichment_process()
        if success:
            print("Proceso de enriquecimiento completado exitosamente.")
            print(f"Datos enriquecidos disponibles en: {enrichment.enriched_csv_path}")
            print(f"Reporte de auditoría disponible en: {enrichment.audit_path}")
        else:
            print("El proceso de enriquecimiento encontró errores. Revise el log para más detalles.")
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        import traceback
        traceback.print_exc()