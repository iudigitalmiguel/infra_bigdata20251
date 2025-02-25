import pandas as pd
import json

def main():
    # Leer el archivo JSON
    with open('data.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Si el JSON es un diccionario único, convertirlo a una lista
    if isinstance(data, dict):
        data = [data]
    
    # Crear DataFrame y guardar en Excel
    df = pd.DataFrame(data)
    df.to_excel('output.xlsx', index=False)
    print("Archivo Excel 'output.xlsx' generado exitosamente.")

if __name__ == '__main__':
    main()