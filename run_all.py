from sproc.core import dl, cli_read_single_zip, cli_rename_columns, read_zips, cli_extend_parquet_with_zip
import argparse
import pathlib
import sys
import yaml

# Script to run the different actions of the sproc package
# It is recommended to run this script from the command line with the following command:
# python3 run_all.py --contract_type outsiders --save_path /path/to/save/place/data 

def main():
    parser = argparse.ArgumentParser(description="sproc")
    #Para tener control sobre las acciones a realizar. Se configura el arg "option" 
    parser.add_argument("--option", default=0, type=int, required=False,
                        help="Option to carry different actions.")
    
    #Para tener control sobre la descarga. OPTION 0
    parser.add_argument("--contract_type", type=str, default="outsiders",
                        required=False, help="Contract types (outsiders, insiders, minors).")
    parser.add_argument("--save_path", type=pathlib.Path, default="../nuevas_descargas",
                        required=False, help="Path to the save download data.")

    #Para tener control sobre leer un solo zip. OPTION 1
    parser.add_argument("--zip_file", type=str, default="PlataformasAgregadasSinMenores_2018.zip",
                        required=False, help="Processing a single zip file")
    parser.add_argument("--output_file", type=str, default="2018.parquet",
                        required=False, help="Dataframe with all metadata in parquet format of a single zip")
    
    #Para tener control sobre el rename de las columnas. OPTION 2
    parser.add_argument("--hierarchical_file", type=str, required=False,
                        help="Hierarchical Parquet file to rename columns.")
    parser.add_argument("--local_file", type=str, required=False,
                        help="Local YAML file for column renaming.")
    parser.add_argument("--repository_file", type=str, required=False,
                        help="Repository file for column renaming.") 
    
    # Argumentos para procesar una lista de archivos zip. OPTION 3
    parser.add_argument("--list_zip_files", nargs='+', type=str, required=False,
                        help="List of zip files to process.")
    parser.add_argument("--output_files", type=str, default="output.parquet", required=False,
                        help="Output file where the DataFrame is to be saved.")
    
    # Argumentos para extender un archivo Parquet con datos de un archivo zip. OPTION 4
    parser.add_argument("--history_file", type=pathlib.Path, required=False,
                        help="Existing Parquet file to extend.")
    parser.add_argument("--zip_file_name", type=pathlib.Path, required=False,
                        help="New zip file with data to append.")
    parser.add_argument("--extended_output_file", type=pathlib.Path, required=False,
                        help="Output Parquet file where the combined data will be saved.")
    
    
    args = parser.parse_args()

    # Opción 0: Descarga
    if args.option == 0:
        dl(args.contract_type, args.save_path)
    
    # Opción 1: Leer un solo archivo zip
    elif args.option == 1: 
        cli_read_single_zip(args.zip_file, args.output_file)
        
    # Opción 2: Renombrar columnas
    elif args.option == 2:
        if not args.hierarchical_file:
            print("Missing hierarchical file parameter for rename columns option.")
            sys.exit(1)
        
        # Comprobamos si se ha proporcionado un archivo YAML local
        if args.local_file:
            local_path = pathlib.Path(args.local_file)
            if not local_path.is_file():
                print(f"The local YAML file {args.local_file} does not exist.")
                sys.exit(1)
            cli_rename_columns(args.hierarchical_file, '-l', args.local_file)
        # En caso de que no se proporcione un archivo YAML local, se busca un archivo YAML remoto
        elif args.repository_file:
            # Aquí podrías añadir código para verificar la existencia del archivo remoto si es necesario
            cli_rename_columns(args.hierarchical_file, '-r', args.repository_file)
        else:
            # Si no se proporciona un archivo YAML, se utiliza un esquema de nomenclatura por defecto
            default_files = ['outsiders.parquet', 'insiders.parquet', 'minors.parquet']
            if pathlib.Path(args.hierarchical_file).name in default_files:
                cli_rename_columns(args.hierarchical_file)
            else:
                print("No YAML file provided and the input file name does not match the default scheme.")
                sys.exit(1)

    # Opción 3: Procesar una lista de archivos zip
    elif args.option == 3:
        if not args.zip_files:
            print("Error: No zip files provided for processing.")
            sys.exit(1)
        
        # Convertir las rutas de archivo a objetos Path y verificar su existencia
        zip_paths = [pathlib.Path(zip_file) for zip_file in args.zip_files]
        for zip_path in zip_paths:
            if not zip_path.exists():
                print(f"Error: The file {zip_path} does not exist.")
                sys.exit(1)
        
        # Llamar a la función read_zips con la lista de archivos zip
        df = read_zips(zip_paths)
        
        # Guardar el DataFrame resultante en el archivo de salida
        output_path = pathlib.Path(args.output_file)
        df.to_parquet(output_path)
        print(f"Data from zip files has been processed and saved to {output_path}")
    
    # Opción 4: Extender un archivo Parquet con datos de un archivo zip
    elif args.option == 4:
        if not args.history_file or not args.zip_file or not args.extended_output_file:
            print("Error: All parameters (--history_file, --zip_file_name, --extended_output_file) are required for option 4.")
            sys.exit(1)
        
        # Verificar que los archivos proporcionados existen
        if not args.history_file.exists() or not args.zip_file.exists():
            print("Error: One or more files do not exist.")
            sys.exit(1)
        
        # Llamar a la función cli_extend_parquet_with_zip con los argumentos adecuados
        cli_extend_parquet_with_zip([
            str(args.history_file),
            str(args.zip_file_name),
            str(args.extended_output_file)
        ])
        print(f"Extended Parquet file created at: {args.extended_output_file}")
    
    else:
        print("Opción no válida. Porfavor seleccione una opción permitida")
        sys.exit(1)

   

if __name__ == '__main__':
    main()
