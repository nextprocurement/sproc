import os
import requests
import json
from bs4 import BeautifulSoup
import argparse
import pandas as pd
import time
from sodapy import Socrata

def download_contracts_gencat(domain, dataset_identifier, destination_directory, file_name):
    # Crear cliente Socrata
    client = Socrata(domain, None)
    
    # Inicializar variables
    offset = 0
    limit = 200000  # Cambia el límite según sea necesario
    results_combined = []
    total_rows = 0

    while True:
        # Obtener un bloque de resultados usando el offset
        results = client.get(dataset_identifier, limit=limit, offset=offset)

        if not results:
            break
        
        # Covert results to a DataFrame
        df = pd.DataFrame.from_records(results)
        results_combined.append(df)
        
        # Update total rows count
        total_rows += len(results)
        print(f"Descargadas {total_rows} filas hasta ahora...")
        
        # Increment offset for the next iteration
        offset += limit
    
    # Combinar todos los DataFrames en uno solo
    full_df = pd.concat(results_combined, ignore_index=True)
    
    # Asegurarse de que el directorio de destino existe, créalo si es necesario
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)
    
    # Guardar el DataFrame combinado en un archivo CSV
    file_path = os.path.join(destination_directory, file_name)
    full_df.to_csv(file_path, index=False)
    print(f"Datos descargados y guardados en {file_path}")

# Auxiliary function to get contract IDs from Zaragoza's API
def get_contract_ids(base_url, params):
    # Initialize a list to store contract details and a counter for downloaded contracts.
    contract_ids = []
    total_downloaded = 0
    
    # Continuously request data until all pages are processed.
    while True:
        response = requests.get(base_url, params=params)
        # Check if the HTTP request was successful.
        if response.status_code != 200:
            print(f"Error de solicitud: {response.status_code}")
            break
        
        # Parse JSON response and extend the contract list with new data.
        data = response.json()
        contract_ids.extend([item['id'] for item in data['result']])
        total_downloaded += len(data['result'])
        
        # Print the progress of downloaded contracts.
        print(f"Descargados {total_downloaded} de {data['totalCount']} contracts.")
        
        # Check if there are more pages of data to request.
        if params['start'] + params['rows'] < data['totalCount']:
            params['start'] += params['rows']  
        else:
            break
           
    return contract_ids 

def download_contracts_zaragoza(contract_ids, detail_url_template, file_path):
    """Descarga los detalles de cada contrato y los guarda en un archivo JSON."""
    contracts = []
    total_contracts = len(contract_ids)
    processed_contracts = 0
    
    for contract_id in contract_ids:
        detail_url = f"{detail_url_template}/{contract_id}.json"
        response = requests.get(detail_url)
        if response.status_code == 200:
            contracts.append(response.json())
        else:
            print(f"Error al obtener detalles del contrato {contract_id}: {response.status_code}")
        
        processed_contracts += 1
        print(f"Procesado {processed_contracts}/{total_contracts} contratos.")
    
    # Guardar los resultados en un archivo JSON
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(contracts, file, ensure_ascii=False, indent=4)
    
    print(f"Detalles de los contratos descargados y guardados en {file_path}")

def download_zaragoza_wrapper(base_url, params, detail_base_url, file_path):
    # Obtain the contract IDs first
    contract_ids = get_contract_ids(base_url, params)
    # Download the details of each contract
    download_contracts_zaragoza(contract_ids, detail_base_url, file_path)
    
def download_contracts_madrid(url, destination_directory, start_year,file_path):
    # Ensure the destination directory exists, create if necessary.
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)

    # Perform an HTTP GET request to retrieve the HTML content of the page.
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful.

    # Parse the HTML using BeautifulSoup to find relevant links.
    soup = BeautifulSoup(response.text, 'html.parser')
    for link in soup.find_all('a'):
        link_text = link.string
        # Filter links that contain the required text.
        if link_text and "Contratos menores inscritos en el Registro de Contratos." in link_text:
            text_parts = link_text.split()
            year = int(text_parts[-1].strip('()'))  # Extract the year from the link text.
            # Check if the year of the contract meets the criteria.
            if year >= start_year:
                href = link.get('href')
                if href:
                    full_url = href if href.startswith('http') else url + href
                    file_name = href.split('/')[-1]
                    # Download the file linked from the URL.
                    file_response = requests.get(full_url)
                    file_response.raise_for_status()  
                    file_path = os.path.join(destination_directory, file_name)
                    with open(file_path, 'wb') as file:
                        file.write(file_response.content)
                    print(f"File {file_name} downloaded in {file_path}")

def download_all_contracts(destination_directory):
    # Parámetros para cada conjunto de datos
    datasets = [
        {
            "func": download_contracts_gencat,
            "params": {
                "domain": "analisi.transparenciacatalunya.cat",
                "dataset_identifier": "ybgg-dgi6",
                "destination_directory": destination_directory,
                "file_name": "contratacion_publica_catalunya_completo.csv"
            }
        },
        {
            "func": download_zaragoza_wrapper,
            "params": {
                "base_url": 'https://www.zaragoza.es/sede/servicio/contratacion-publica/contrato.json',
                "params": {
                    'procedimiento.id': '10',
                    'fl': 'id',
                    'start': 0,
                    'rows': 50
                },
                "detail_base_url": "https://www.zaragoza.es/sede/servicio/contratacion-publica",
                "file_path": os.path.join(destination_directory, "contratos_zaragoza_detalles.json")
            }
        },
        {
            "func": download_contracts_madrid,
            "params": {
                "url": 'https://transparencia.madrid.es/portales/transparencia/es/Economia-y-presupuestos/Contratacion/Contratacion-administrativa/Contratos-menores/?vgnextfmt=default&vgnextoid=6f86ad4dd90f9710VgnVCM1000001d4a900aRCRD&vgnextchannel=cd7079a1180d9710VgnVCM1000001d4a900aRCRD',
                "destination_directory": destination_directory,
                "start_year": 2018,
                "file_path": "DESCARGAS/"
            }
        }
    ]    
    # Descargar cada dataset
    for dataset in datasets:
        func = dataset["func"]
        params = dataset["params"]
        print(f"Descargando datos de {func.__name__}...")
        func(**params)

def main():
    parser = argparse.ArgumentParser(description="Download minor contracts from various cities.")
    parser.add_argument("city", choices=['zaragoza', 'madrid', 'gencat', 'all'], help="Specify the city for the download: zaragoza, madrid, gencat, or all")
    parser.add_argument("--start_year", type=int, default=2018, help="Year from which to start downloads for Madrid")
    parser.add_argument("--file_path", help="Path to the file or directory to save the data", default="/export/usuarios_ml4ds/cggamella/sproc/DESCARGAS")
    args = parser.parse_args()

    if args.city == 'zaragoza':
        # Configuraciones específicas para Zaragoza
        base_url = 'https://www.zaragoza.es/sede/servicio/contratacion-publica/contrato.json'
        params = {
            'procedimiento.id': '10',
            'fl': 'id',
            'start': 0,
            'rows': 50
        }
        detail_base_url = 'https://www.zaragoza.es/sede/servicio/contratacion-publica'
        file_path = os.path.join(args.file_path, "contratos_menores_zaragoza.json")
        download_zaragoza_wrapper(base_url, params, detail_base_url, file_path)
            
    elif args.city == 'madrid':
        start_url = 'https://transparencia.madrid.es/portales/transparencia/es/Economia-y-presupuestos/Contratacion/Contratacion-administrativa/Contratos-menores'
        if args.file_path:
            download_contracts_madrid(start_url, args.file_path, args.start_year)
    
    elif args.city == 'gencat':
        domain = "analisi.transparenciacatalunya.cat"
        dataset_identifier = "ybgg-dgi6"
        if args.file_path:
            download_contracts_gencat(domain, dataset_identifier, args.file_path, "contratacion_publica_catalunya_completo1.csv")
    elif args.city == 'all':
        if args.file_path:
            download_all_contracts(args.file_path)

if __name__ == "__main__":
    main()
    
# Descarga de Contratos Menores Zgz y Madrid, y datos de la Generalitat de Catalunya
# Este script permite la descarga automatizada de contratos menores desde los portales de transparencia
# de Zaragoza y Madrid. Además de los contratos de la Generalitat de Catalunya. Está la opción de descargar
# todos los conjuntos de datos a la vez.

## Requirements
#- Requests: 'pip install requests'
#- BeautifulSoup4: 'pip install beautifulsoup4'
#- Sodapy: 'pip install sodapy'

## Uso
# El script se puede ejecutar desde la línea de comandos. Debes especificar la ciudad y la ruta donde deseas guardar los datos.
# Además, para Madrid, puedes especificar el año inicial desde el que deseas comenzar las descargas.

### Ejemplos de Uso
#Descargar contratos menores de Zaragoza y guardarlos en un archivo JSON:
#python3 descarga_opendata.py zaragoza --ruta_archivo /path/to/contratos_menores_zaragoza.json

# Descargar contratos menores de Zaragoza
#python3 descarga_opendata.py zaragoza --file_path /path/to/contratos_menores_zaragoza.json
# Descargar contratos menores de Madrid desde 2018
#python3 descarga_opendata.py madrid --start_year 2018 --file_path /path/to/contratos_menores_madrid
# Descargar datos de contratación pública de Cataluña
#python3 descarga_opendata.py gencat --file_path /path/to/contratacion_publica_catalunya.csv
# Descargar todos los conjuntos de datos
#python3 descarga_opendata.py all --file_path /path/to/descargas
