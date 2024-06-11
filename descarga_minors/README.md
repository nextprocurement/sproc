Este código en Python está diseñado para descargar y *analizar* los datos de contratos menores de los portales de transparencia de los Ayuntamientos de Zaragoza y Madrid. Los resultados se guardan en archivos en formato JSON que pueden ser leídos fácilmente en muchos lenguajes de programación.

Este proyecto se desarrolló con herramientas estándar de Python, y cada función está documentada para facilitar su comprensión y uso. Si te interesa conocer el funcionamiento interno de alguna función, puedes revisar el código directamente en el archivo fuente.

## Instalación

Para ejecutar este script, necesitarás instalar las siguientes librerías de Python:

    pip install requests beautifulsoup4

## Cómo usar

El software se puede utilizar como un script independiente desde la línea de comandos.

### Scripts

#### Descargando datos

El comando principal para ejecutar el script es el siguiente, donde debes especificar la ciudad y la ruta donde se guardarán los datos. Para Madrid, también puedes especificar desde qué año deseas comenzar las descargas:

    python script.py zaragoza --file_path /ruta/a/contratos_menores_zaragoza.json
    python script.py madrid --file_path /ruta/a/contratos_menores_madrid --start_year 2018

### Funcionamiento detallado

El script tienes las funciones de `download_contracts_zaragoza` y `download_contracts_madrid` que son las funciones principales para descargar los datos de contratos menores de Zaragoza y Madrid respectivamente. Estas funciones gestionan la paginación de los datos y aseguran la integridad de las descargas mediante la comprobación de errores en las respuestas HTTP.

#### Procesando un solo archivo JSON

Por ejemplo, al ejecutar el script para Zaragoza, se descargan todos los contratos menores disponibles y se escriben en un archivo JSON especificado. El argumento `--file_path` puede usarse para especificar un directorio diferente al actual.

#### Procesando datos de Madrid

Para Madrid, el script descarga archivos directamente desde los enlaces encontrados en la página de transparencia. Puedes especificar el año inicial con `--start_year`, y solo se descargarán los contratos de ese año en adelante. Por defectose descargan a partir del 2018 incluido.
