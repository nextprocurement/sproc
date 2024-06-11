#!/bin/bash

AGREGADOS_FILES=( 
    # yearly
    PlataformasAgregadasSinMenores_2018.zip
    PlataformasAgregadasSinMenores_2019.zip
    PlataformasAgregadasSinMenores_2020.zip
    PlataformasAgregadasSinMenores_2021.zip
    # monthly
    PlataformasAgregadasSinMenores_202201.zip
    PlataformasAgregadasSinMenores_202202.zip
    PlataformasAgregadasSinMenores_202203.zip
    PlataformasAgregadasSinMenores_202204.zip
    PlataformasAgregadasSinMenores_202205.zip
    PlataformasAgregadasSinMenores_202206.zip
    PlataformasAgregadasSinMenores_202207.zip
    PlataformasAgregadasSinMenores_202208.zip
    PlataformasAgregadasSinMenores_202209.zip
    PlataformasAgregadasSinMenores_202210.zip
)

MENORES_FILES=( 
    # yearly
    contratosMenoresPerfilesContratantes_2018.zip
    contratosMenoresPerfilesContratantes_2019.zip
    contratosMenoresPerfilesContratantes_2020.zip
    contratosMenoresPerfilesContratantes_2021.zip
    # monthly
    contratosMenoresPerfilesContratantes_202201.zip
    contratosMenoresPerfilesContratantes_202202.zip
    contratosMenoresPerfilesContratantes_202203.zip
    contratosMenoresPerfilesContratantes_202204.zip
    contratosMenoresPerfilesContratantes_202205.zip
    contratosMenoresPerfilesContratantes_202206.zip
    contratosMenoresPerfilesContratantes_202207.zip
    contratosMenoresPerfilesContratantes_202208.zip
    contratosMenoresPerfilesContratantes_202209.zip
    contratosMenoresPerfilesContratantes_202210.zip
)

PERFILES_PLATAFORMA_FILES=( 
    # yearly
    licitacionesPerfilesContratanteCompleto3_2018.zip
    licitacionesPerfilesContratanteCompleto3_2019.zip
    licitacionesPerfilesContratanteCompleto3_2020.zip
    licitacionesPerfilesContratanteCompleto3_2021.zip
    # monthly
    licitacionesPerfilesContratanteCompleto3_202201.zip
    licitacionesPerfilesContratanteCompleto3_202202.zip
    licitacionesPerfilesContratanteCompleto3_202203.zip
    licitacionesPerfilesContratanteCompleto3_202204.zip
    licitacionesPerfilesContratanteCompleto3_202205.zip
    licitacionesPerfilesContratanteCompleto3_202206.zip
    licitacionesPerfilesContratanteCompleto3_202207.zip
    licitacionesPerfilesContratanteCompleto3_202208.zip
    licitacionesPerfilesContratanteCompleto3_202209.zip
    licitacionesPerfilesContratanteCompleto3_202210.zip
)

download () {

    # arguments are processed:
    
    # - output directory, where the files will be downloaded
    OUTPUT_DIR=$1
    
    # - base URL, where the files are hosted in the remote server
    URL_BASE=$2
    
    # - list of files
    local -n FILES=$3
    
    mkdir -p $OUTPUT_DIR
    
    pushd $OUTPUT_DIR
  
    for f in "${FILES[@]}"
    do
        
        # if it already exists...
        if test -f "$f"; then
        
            echo skipping \""$f"\"...it already exists
        
        # if the directory does NOT exist...
        else
        
            FULL_URL=$URL_BASE/$f
        
            echo =================== downloading \""$f"\"  ===================
            
            wget $FULL_URL
        
        fi
        
    done
    
    popd
}

download data/agregados https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044 AGREGADOS_FILES
download data/menores https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1143 MENORES_FILES
download data/perfiles_plataforma https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643 PERFILES_PLATAFORMA_FILES
