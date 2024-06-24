# Script de Descarga de Datos de Contratación

Este script en Python facilita la descarga automatizada de datos de contratos menores desde los portales de transparencia de los Ayuntamientos de Zaragoza y Madrid, así como de la Generalitat de Catalunya. Además, ofrece la opción de descargar todos los conjuntos de datos de manera simultánea.

## Requisitos

Para utilizar este script, es necesario instalar las siguientes bibliotecas de Python:

- **Requests**: Permite realizar solicitudes HTTP.
- **BeautifulSoup4**: Utilizado para parsear documentos HTML.
- **Pandas**: Proporciona herramientas para la manipulación y análisis de datos.
- **Sodapy**: Facilita la interacción con APIs que emplean el protocolo Socrata Open Data (SODA).

## Instalación de las bibliotecas

Ejecute el siguiente comando para instalar todas las dependencias necesarias:

pip install requests beautifulsoup4 pandas sodapy

#### Ejemplos de Comandos

- **Zaragoza** - Descargar contratos menores y guardarlos en un archivo JSON:

 python3 script.py zaragoza --file_path /ruta/a contratos_menores_zaragoza.json

- **Madrid** - Descargar contratos menores desde el año 2018:

 python3 script.py madrid --start_year 2018 --file_path /ruta/a/contratos_menores_madrid

- **Cataluña** - Descargar datos de contratación pública:

 python3 script.py gencat --file_path /ruta/a/contratacion_publica_catalunya.csv

El comando principal para ejecutar el script y descargar simultáneamente todos los datos es el siguiente:

 python3 script.py all --file_path /ruta/a/destino/descargas

### Funcionamiento detallado

El script incluye funciones específicas para la descarga de datos:

- download_contracts_gencat: Descarga datos desde la Generalitat de Catalunya.
- download_contracts_zaragoza: Realiza solicitudes a la API de Zaragoza y almacena los resultados en formato JSON.
- download_contracts_madrid: Descarga archivos directamente desde la página de transparencia de Madrid basados en el año de publicación.
- download_all_contracts: Coordina la descarga de todos los datos mencionados utilizando las funciones correspondientes.
