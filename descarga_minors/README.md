# Script de Descarga de Datos de Contratación

Este script en Python facilita la descarga automatizada de datos de contratos menores desde los portales de transparencia de los Ayuntamientos de Zaragoza y Madrid, así como de la Generalitat de Catalunya de manera independiente. Además, ofrece la opción de descargar todos los conjuntos de datos de manera simultánea.

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

El comando principal para ejecutar el script y descargar simultáneamente todos los datos es el siguiente:

 python3 descarga_contratos_mad_zgz_gencat.py all --file_path /ruta/a/destino/descargas

 También existe la opción de hacer la descarga de forma independiente para cada administración:
 
- **Zaragoza** - Descargar contratos menores y guardarlos en un archivo JSON:

 python3 descarga_contratos_mad_zgz_gencat.py zaragoza --file_path /ruta/a contratos_menores_zaragoza.json

- **Madrid** - Descargar contratos menores desde el año 2018:

 python3 descarga_contratos_mad_zgz_gencat.py madrid --start_year 2018 --file_path /ruta/a/contratos_menores_madrid

- **Cataluña** - Descargar datos de contratación pública:

 python3 descarga_contratos_mad_zgz_gencat.py gencat --file_path /ruta/a/contratacion_publica_catalunya.csv

### Funcionamiento detallado

El script incluye funciones específicas para la descarga de datos:

- download_contracts_gencat: Descarga datos desde la Generalitat de Catalunya.
- get_contract_ids: Es una función auxiliar para recoger los id de la API de Zaragoza
- download_contracts_zaragoza: Realiza solicitudes a la API de Zaragoza y almacena los resultados en formato JSON en 2 pasos, una primera función que interactúa con la API para recoger todos los id cuyo procedimiento.id es 10 (indica contrato menor) con la función get_contract_ids y una segunda llamada a otro endpoint para obtener la información completa de los contratos.
- download_zaragoza_wrapper: Actúa como una función wrapper para hacer la descarga de zaragoza en 2 pasos.
- download_contracts_madrid: Descarga archivos directamente desde la página de transparencia de Madrid basados en el año de publicación.
- download_all_contracts: Coordina la descarga de todos los datos mencionados utilizando las funciones correspondientes.
