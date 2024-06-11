import os
import requests
import json
from bs4 import BeautifulSoup
import argparse

def download_contracts_zaragoza(base_url, params, file_path):
    # Initialize a list to store contract details and a counter for downloaded contracts.
    contracts = []
    total_downloaded = 0

    # Continuously request data until all pages are processed.
    while True:
        response = requests.get(base_url, params=params)
        # Check if the HTTP request was successful.
        if response.status_code != 200:
            print(f"Request error: {response.status_code}")
            break

        # Parse JSON response and extend the contract list with new data.
        data = response.json()
        contracts.extend(data['result'])
        total_downloaded += len(data['result'])

        # Print the progress of downloaded contracts.
        print(f"Downloaded {total_downloaded} of {data['totalCount']} contracts.")

        # Check if there are more pages of data to request.
        if params['start'] + params['rows'] < data['totalCount']:
            params['start'] += params['rows']  # Prepare params for the next page.
        else:
            break

    # Write all gathered contract data into a specified file in JSON format.
    with open(file_path, 'w') as file:
        json.dump(contracts, file, ensure_ascii=False, indent=4)

    # Return the list of downloaded contracts for further processing or verification.
    return contracts

def download_contracts_madrid(url, destination_directory, start_year):
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
                    file_response.raise_for_status()  # Ensure the download was successful.
                    file_path = os.path.join(destination_directory, file_name)
                    with open(file_path, 'wb') as file:
                        file.write(file_response.content)
                    print(f"File {file_name} downloaded in {file_path}")

def main():
    parser = argparse.ArgumentParser(description="Download minor contracts from Zaragoza or Madrid.")
    parser.add_argument("city", choices=['zaragoza', 'madrid'], help="Specify the city for the download: zaragoza or madrid")
    parser.add_argument("--start_year", type=int, default=2018, help="Year from which to start downloads for Madrid")
    parser.add_argument("--file_path", help="Path to the file or directory to save the data")
    args = parser.parse_args()

    if args.city == 'zaragoza':
        base_url = 'https://www.zaragoza.es/sede/servicio/contratacion-publica/contrato.json'
        parameters = {'contratoMenor': 'true', 'start': 0, 'rows': 50}
        if args.file_path:
            download_contracts_zaragoza(base_url, parameters, args.file_path)
    elif args.city == 'madrid':
        start_url = 'https://transparencia.madrid.es/portales/transparencia/es/Economia-y-presupuestos/Contratacion/Contratacion-administrativa/Contratos-menores/?vgnextfmt=default&vgnextoid=6f86ad4dd90f9710VgnVCM1000001d4a900aRCRD&vgnextchannel=cd7079a1180d9710VgnVCM1000001d4a900aRCRD'
        if args.file_path:
            download_contracts_madrid(start_url, args.file_path, args.start_year)

if __name__ == "__main__":
    main()
    
# Descarga de Contratos Menores Zgz y Madrid
# Este script permite la descarga automatizada de contratos menores desde los portales de transparencia
# de Zaragoza y Madrid.

## Requirements
#- Requests: `pip install requests`
#- BeautifulSoup4: `pip install beautifulsoup4`

## Uso
# El script se puede ejecutar desde la línea de comandos. Debes especificar la ciudad y la ruta donde deseas guardar los datos.
# Además, para Madrid, puedes especificar el año inicial desde el que deseas comenzar las descargas.

### Ejemplos de Uso
#Descargar contratos menores de Zaragoza y guardarlos en un archivo JSON:
#python script.py zaragoza --ruta_archivo /path/to/contratos_menores_zaragoza.json