from pathlib import Path
from datetime import datetime
from dateutil import parser
import pandas as pd
import numpy as np
import argparse
import sys
import yaml
import numbers
import requests
import json
import ast
import time
import re
import json
import os
import logging

# Configuración del logging
logging.basicConfig(
    filename='app.log',  
    filemode='a', 
    level=logging.DEBUG,  
    format='%(asctime)s - %(levelname)s - %(message)s'  
)
# Auxiliar functions for Zaragoza's Open Data integration
def extract_field(data, key):
    """ 
    Extracts a value from a dictionary using the provided key.
    """
    try:
        return data[key]
    except (TypeError, KeyError):
        return None

def extract_from_list_of_dicts(lst, key):
    """
    Extracts a value from the first dictionary in a list using the specified key.
    """
    if lst and isinstance(lst, list):
        return lst[0].get(key, None)
    return None

def extract_nested_field(data, path):
    """"
    Attempts to retrieve a value from a nested structure of dictionaries/lists using a list of keys/indices to navigate the path.
    """
    for key in path:
        try:
            data = data[key]
        except (TypeError, KeyError, IndexError):
            return None
    return data

def extract_nested_field_from_list(lst, path):
    """
    Extracts a nested field from a list of dictionaries, following a specified path of keys.
    """
    if lst and isinstance(lst, list):
        for item in lst:
            data = item
            try:
                for key in path:
                    data = data[key]
                return data
            except (TypeError, KeyError, IndexError):
                continue
    return None

def extract_uris(anuncios):
    uris_3 = []
    uris_4 = []
    if isinstance(anuncios, list):
        for anuncio in anuncios:
            if 'type' in anuncio:
                type_id = anuncio['type'].get('id')
                uri = anuncio.get('uri', None)
                # Verifica si 'id' es 3 y guarda la URI correspondiente
                if type_id == 3 and uri:
                    uris_3.append(uri)
                # Verifica si 'id' es 4 y guarda la URI correspondiente
                elif type_id == 4 and uri:
                    uris_4.append(uri)
                    
    # Formatea las listas de URIs a cadenas separadas por comas, o utiliza pd.NA si la lista está vacía
    uris_3_str = ', '.join(uris_3) if uris_3 else pd.NA
    uris_4_str = ', '.join(uris_4) if uris_4 else pd.NA
    
    return {'anuncio.type.id.3.uri': uris_3_str, 'anuncio.type.id.4.uri': uris_4_str}

id_to_place_mapping = {
    0: np.nan,
    1: np.nan,
    2: 1,
    3: 2,
    4: 4,
    5: 8,
    6: 9,
    7: 3,
    8: 5,
    9: 9,
    10: 4,
    11: 4,
    12: 9,
    13: 8,
    14: 9
}

title_to_place_mapping = {
    "En Licitación": "PUB",
    "Pendiente de Adjudicar": "EV",
    "Adjudicación Provisional": "ADJ",
    "Adjudicación Definitiva": "ADJ",
    "Suspendida la licitación": "ANUL",
    "Adjudicación": "ADJ",
    "Contrato Formalizado": "RES",
    "Desierto": "RES",
    "Renuncia": "RES",
    "Modificación del Contrato": "RES",
    "Cancelado": "ANUL",
    "Desistido": "ANUL",
    "Resuelto": "RES",
    "Parcialmente Adjudicado": "ADJ",
    "Parcialmente Formalizado": "ADJ"
}

def convert_to_ndarray(item):
    return np.array([str(item)], dtype=object)

# Following https://contrataciondelestado.es/codice/cl/2.07/SyndicationTenderingProcessCode-2.07.gc
def substitute_contract_value(x):
    if x == 'Contracte menor':
        return 6.0
    
    elif x == 'Restringit':
        return 2.0
    
    elif x == 'Altres procediments segons instruccions internes':
        return 999.0
    
    else:  
        return x

# Following https://contrataciondelestado.es/codice/cl/2.07/SyndicationContractCode-2.07.gc
def substitute_type_code(x):
    if x == 'Subministraments':
        return 1.0
    elif x == 'Serveis' or x == 'Contracte de serveis especials (annex IV)' or x == "Concessió de serveis especials (annex IV)":
        return 2.0
    elif x == 'Obres':
        return 3.0
    elif x == "Concessió de serveis":
        return 22.0
    elif x == "Concessió d'obres":
        return 32.0
    elif x == 'Administratiu especial':
        return 7.0
    elif x == "Privat d'Administració Pública":
        return 40.0
    elif x == 'Administratiu especial':
        return 7.0
    elif x == 'Altra legislació sectorial':
        return 999.0
    else:
        return x
    
def extraer_url(row):
    link_str = row['link']
    try:
        link_dict = ast.literal_eval(link_str)
        return link_dict.get('url', link_str)
    except (ValueError, SyntaxError):
        return link_str
    
def convert_date_to_array(date_str):
    date_str = str(date_str)
    
    if date_str == 'nan':
        return np.array([None], dtype=object)

    date_formats = ['%d/%m/%Y', '%d/%m/%y', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']
    
    for format in date_formats:
        try:
            date_object = datetime.strptime(date_str, format)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"No se pudo parsear la fecha: {date_str}")
    
    # Formatear la fecha a 'yyyy-mm-dd'
    formatted_date = date_object.strftime('%Y-%m-%d')
    
    # Convertir la fecha formateada a un numpy.ndarray con dtype=object
    return np.array([formatted_date], dtype=object)

def float64_to_array(item):
    # Convertir el item a float64 si no lo es
    if not isinstance(item, np.float64):
        item = np.float64(item)
    # Convertir el número a cadena y guardarlo en un numpy.ndarray con dtype=object
    return np.array([str(item)], dtype=object)

def convert_int64_to_float64(value):
    return np.float64(value)

def convert_to_object_array(x):
    if isinstance(x, list):  
        return np.array(x, dtype=object)
    elif isinstance(x, (str, float, int)):
        return np.array([x], dtype=object)
    elif not isinstance(x, np.ndarray): 
        return np.array([x], dtype=object)
    return x

def convert_to_timestamp(date_str, tz='UTC'):
    if not isinstance(date_str, str):
        return pd.NaT  
    try:
        parsed_date = parser.parse(date_str)
        timestamp = pd.Timestamp(parsed_date, tz=tz)
        return timestamp
    except ValueError:
        return pd.NaT

def ensure_array(x):
    if isinstance(x, np.ndarray):
        return x
    elif pd.isnull(x):
        return np.array([None], dtype=object)
    elif isinstance(x, (list, tuple)):
        return np.array(x, dtype=object)
    elif isinstance(x, (int, float)):
        return np.array([str(x)], dtype=object)
    else:
        return np.array([x], dtype=object)

# Mapeo de columnas para Zaragoza
mapeo_zgz = {
    'url': 'link',
    'title': 'title',
    'expediente': 'ContractFolderStatus.ContractFolderID',
    'organoContratante.dir3': 'ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID',
    'organoContratante.title': 'ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name',
    'organoContratante.address.locality': 'ContractFolderStatus.ProcurementProject.RealizedLocation.Address.CityName',
    'organoContratante.postal_code': 'ContractFolderStatus.ProcurementProject.RealizedLocation.Address.PostalZone',
    'cpv.id': 'ContractFolderStatus.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode',
    'type.id': 'ContractFolderStatus.ProcurementProject.TypeCode',
    'importeConIVA': 'ContractFolderStatus.ProcurementProject.BudgetAmount.TotalAmount',
    'importeSinIVA': 'ContractFolderStatus.ProcurementProject.BudgetAmount.TaxExclusiveAmount',  
    'servicio.address.locality': 'ContractFolderStatus.ProcurementProject.RealizedLocation.CountrySubentity',
    'servicio.address.countryName': 'ContractFolderStatus.ProcurementProject.RealizedLocation.Address.Country.Name',
    'duracion': 'ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure',
    'status.title': 'ContractFolderStatus.TenderResult.StatusCode',
    'status.id': 'ContractFolderStatus.TenderResult.ResultCode',
    'fechaContrato': 'ContractFolderStatus.TenderResult.AwardDate',
    'numLicitadores': 'ContractFolderStatus.TenderResult.ReceivedTenderQuantity',
    'ofertas.empresa.nif': 'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.ID',
    'ofertas.empresa.nombre': 'ContractFolderStatus.TenderResult.WinningParty.PartyName.Name',
    'ofertas.importeSinIVA': 'ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount',
    'ofertas.importeConIVA': 'ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.PayableAmount',
    'pubDate': 'ContractFolderStatus.ValidNoticeInfo.AdditionalPublicationStatus.AdditionalPublicationDocumentReference.IssueDate',
    'procedimiento.id': 'ContractFolderStatus.TenderingProcess.ProcedureCode',
    'criterio.tipo.title': 'ContractFolderStatus.TenderingTerms.AwardingTerms.AwardingCriteria.AwardingCriteriaTypeCode',
    'criterio.title': 'ContractFolderStatus.TenderingTerms.AwardingTerm.AwardingCriteria.Description',
    'criterio.peso': 'ContractFolderStatus.TenderingTerms.AwardingTerms.AwardingCriteria.WeightNumeric',
    'valorEstimado': 'ContractFolderStatus.ProcurementProject.BudgetAmount.EstimatedOverallContractAmount',
    'entity.title': 'ContractFolderStatus.LocatedContractingParty.ParentLocatedParty.PartyName.Name',
    'fechaContrato': 'ContractFolderStatus.TenderResult.Contract.IssueDate',
    'anuncio.type.id.4.uri': 'ContractFolderStatus.LegalDocumentReference.Attachment.ExternalReference.URI',
    'anuncio.type.id.3.uri': 'ContractFolderStatus.TechnicalDocumentReference.Attachment.ExternalReference.URI',
    'fechaPresentacion': 'ContractFolderStatus.TenderingProcess.TenderSubmissionDeadlinePeriod.EndDate',
    'lastUpdated': 'updated'
}

def process_zaragoza(df, df_minors):
    """
    Process the Zaragoza's Open Data DataFrame to extract and transform the relevant fields.
    
    Return: df
    With the relevant fields concatenated with the df_minors DataFrame.
    """
    # Extracting the fields of column 'type'
    df['type.id'] = df['type'].apply(lambda x: extract_field(x, 'id'))
    df['type.title'] = df['type'].apply(lambda x: extract_field(x, 'title'))
    df['type.type'] = df['type'].apply(lambda x: extract_field(x, 'type'))

    # Extracting the fields of column 'procedimiento'
    df['procedimiento.id'] = df['procedimiento'].apply(lambda x: extract_field(x, 'id'))
    df['procedimiento.nombre'] = df['procedimiento'].apply(lambda x: extract_field(x, 'nombre'))

    # Extracting the fields of column 'entity'
    df['entity.id'] = df['entity'].apply(lambda x: extract_field(x, 'id'))
    df['entity.title'] = df['entity'].apply(lambda x: extract_field(x, 'title'))
    df['entity.lastUpdated'] = df['entity'].apply(lambda x: extract_field(x, 'lastUpdated'))
    df['entity.schema'] = df['entity'].apply(lambda x: extract_field(x, 'schema'))
    df['entity.idSchema'] = df['entity'].apply(lambda x: extract_field(x, 'idSchema'))

    # Extracting the fields of column 'status'
    df['status.id'] = df['status'].apply(lambda x: extract_field(x, 'id'))
    df['status.title'] = df['status'].apply(lambda x: extract_field(x, 'title'))

    # Extracting the fields of column 'cpv'
    df['cpv.id'] = df['cpv'].apply(lambda x: extract_from_list_of_dicts(x, 'id'))
    df['cpv.titulo'] = df['cpv'].apply(lambda x: extract_from_list_of_dicts(x, 'titulo'))

    # Extracting the fields of column 'servicio'
    df['servicio.id'] = df['servicio'].apply(lambda x: extract_field(x, 'id'))
    df['servicio.dir3'] = df['servicio'].apply(lambda x: extract_field(x, 'dir3'))
    df['servicio.title'] = df['servicio'].apply(lambda x: extract_field(x, 'title'))
    df['servicio.phone'] = df['servicio'].apply(lambda x: extract_field(x, 'phone'))
    df['servicio.postal_code'] = df['servicio'].apply(lambda x: extract_field(x, 'postal_code'))
    df['servicio.nivelAdministracion'] = df['servicio'].apply(lambda x: extract_field(x, 'nivelAdministracion'))
    df['servicio.tipoEntidadPublica'] = df['servicio'].apply(lambda x: extract_field(x, 'tipoEntidadPublica'))
    df['servicio.nivelJerarquico'] = df['servicio'].apply(lambda x: extract_field(x, 'nivelJerarquico'))
    df['servicio.unidadSuperior'] = df['servicio'].apply(lambda x: extract_field(x, 'unidadSuperior'))
    df['servicio.unidadRaiz'] = df['servicio'].apply(lambda x: extract_field(x, 'unidadRaiz'))
    df['servicio.status'] = df['servicio'].apply(lambda x: extract_field(x, 'status'))
    df['servicio.headOf'] = df['servicio'].apply(lambda x: extract_field(x, 'headOf'))
    df['servicio.creationDate'] = df['servicio'].apply(lambda x: extract_field(x, 'creationDate'))
        
    # Extracting the fields of column 'organoContratante'
    df['organoContratante.id'] = df['organoContratante'].apply(lambda x: extract_field(x, 'id'))
    df['organoContratante.dir3'] = df['organoContratante'].apply(lambda x: extract_field(x, 'dir3'))
    df['organoContratante.title'] = df['organoContratante'].apply(lambda x: extract_field(x, 'title'))
    df['organoContratante.phone'] = df['organoContratante'].apply(lambda x: extract_field(x, 'phone'))
    df['organoContratante.postal_code'] = df['organoContratante'].apply(lambda x: extract_field(x, 'postal_code'))

    df['organoContratante.nivelAdministracion'] = df['organoContratante'].apply(lambda x: extract_field(x, 'nivelAdministracion'))
    df['organoContratante.tipoEntidadPublica'] = df['organoContratante'].apply(lambda x: extract_field(x, 'tipoEntidadPublica'))
    df['organoContratante.nivelJerarquico'] = df['organoContratante'].apply(lambda x: extract_field(x, 'nivelJerarquico'))
    df['organoContratante.unidadSuperior'] = df['organoContratante'].apply(lambda x: extract_field(x, 'unidadSuperior'))
    df['organoContratante.unidadRaiz'] = df['organoContratante'].apply(lambda x: extract_field(x, 'unidadRaiz'))
    df['organoContratante.status'] = df['organoContratante'].apply(lambda x: extract_field(x, 'status'))
    df['organoContratante.headOf'] = df['organoContratante'].apply(lambda x: extract_field(x, 'headOf'))
    df['organoContratante.creationDate'] = df['organoContratante'].apply(lambda x: extract_field(x, 'creationDate'))

    fields = {
        'address.id': ['address', 'id'],
        'address.address': ['address', 'address'],
        'address.postal_code': ['address', 'postal_code'],
        'address.locality': ['address', 'locality'],
        'address.countryName': ['address', 'countryName'],
        'address.geometry.type': ['address', 'geometry', 'type'],
        'address.geometry.coordinates': ['address', 'geometry', 'coordinates'],
    }

    # Extracting the fields nested at 'servicio.address' and  'organoContratante.address'
    for field, path in fields.items():
        df[f'servicio.{field}'] = df['servicio'].apply(lambda x: extract_nested_field(x, path))
        df[f'organoContratante.{field}'] = df['organoContratante'].apply(lambda x: extract_nested_field(x, path))

    # Extracting the fields of 'criterios' field
    df['criterio.id'] = df['criterios'].apply(lambda x: extract_from_list_of_dicts(x, 'idCriterio'))
    df['criterio.description'] = df['criterios'].apply(lambda x: extract_from_list_of_dicts(x, 'description'))
    df['criterio.title'] = df['criterios'].apply(lambda x: extract_from_list_of_dicts(x, 'title'))
    df['criterio.peso'] = df['criterios'].apply(lambda x: extract_from_list_of_dicts(x, 'peso'))
    # Extracting the fields nested in 'criterios.tipo' -> 'id' and 'title'
    df['criterio.tipo.id'] = df['criterios'].apply(lambda x: extract_nested_field_from_list(x, ['tipo', 'id']))
    df['criterio.tipo.title'] = df['criterios'].apply(lambda x: extract_nested_field_from_list(x, ['tipo', 'title']))

    # Extracting the fields of 'ofertas' field
    df['ofertas.id'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'id'))
    df['ofertas.fechaAdjudicacion'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'fechaAdjudicacion'))
    df['ofertas.fechaFormalizacion'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'fechaFormalizacion'))
    df['ofertas.importeSinIVA'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'importeSinIVA'))
    df['ofertas.canon'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'canon'))
    df['ofertas.autonomo'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'autonomo'))
    df['ofertas.ganador'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'ganador'))
    df['ofertas.importeConIVA'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'importeConIVA'))
    df['ofertas.iva'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'iva'))
    df['ofertas.precioUnitario'] = df['ofertas'].apply(lambda x: extract_from_list_of_dicts(x, 'precioUnitario'))

    # Extracting nested fields in 'ofertas.empresa' REVISAR PORQUE EN OCASIONES TIENEN MÁS DE UNA ENTRADA
    df['ofertas.empresa.idEmpresa'] = df['ofertas'].apply(lambda x: extract_nested_field_from_list(x, ['empresa', 'idEmpresa']))
    df['ofertas.empresa.nombre'] = df['ofertas'].apply(lambda x: extract_nested_field_from_list(x, ['empresa', 'nombre']))
    df['ofertas.empresa.nif'] = df['ofertas'].apply(lambda x: extract_nested_field_from_list(x, ['empresa', 'nif']))
    df['ofertas.empresa.openCorporateUrl'] = df['ofertas'].apply(lambda x: extract_nested_field_from_list(x, ['empresa', 'openCorporateUrl']))
    df['ofertas.empresa.esUte'] = df['ofertas'].apply(lambda x: extract_nested_field_from_list(x, ['empresa', 'esUte']))
    df['ofertas.empresa.autonomo'] = df['ofertas'].apply(lambda x: extract_nested_field_from_list(x, ['empresa', 'autonomo']))
    uris = df['anuncios'].apply(extract_uris)
    df['anuncio.type.id.3.uri'] = uris.apply(lambda x: x['anuncio.type.id.3.uri'])
    df['anuncio.type.id.4.uri'] = uris.apply(lambda x: x['anuncio.type.id.4.uri'])
    # Remove original cols where data where nested
    df.drop(['type','ofertas','procedimiento','entity','status','cpv','servicio','criterios','organoContratante'], axis=1, inplace=True)
    df['cpv.id'] = df['cpv.id'].apply(float64_to_array)
    df['status.id'] = df['status.id'].map(id_to_place_mapping).astype(np.float64)
    df['status.title'] = df['status.title'].map(title_to_place_mapping)
    df['status.id'] = df['status.id'].apply(convert_to_ndarray)
    df['status.title'] = df['status.title'].apply(convert_to_ndarray)

    df['fechaContrato'] = df['fechaContrato'].apply(convert_date_to_array)
    df['numLicitadores'] = df['numLicitadores'].apply(float64_to_array)
    df['ofertas.empresa.nif'] = df['ofertas.empresa.nif'].apply(convert_to_ndarray)
    df['ofertas.empresa.nombre'] = df['ofertas.empresa.nombre'].apply(convert_to_ndarray)
    df['ofertas.importeSinIVA'] = df['ofertas.importeSinIVA'].apply(float64_to_array)
    df['ofertas.importeConIVA'] = df['ofertas.importeConIVA'].apply(float64_to_array)
    df['pubDate'] = df['pubDate'].apply(convert_date_to_array)
    df['procedimiento.id'] = df['procedimiento.id'].apply(convert_int64_to_float64)

    # Criterios, no está en minors ni outsiders (solo en insiders)
    df['criterio.tipo.title'] = df['criterio.tipo.title'].apply(convert_to_ndarray)
    df['criterio.title'] = df['criterio.title'].apply(convert_to_ndarray)
    df['criterio.peso'] = df['criterio.peso'].apply(convert_to_ndarray)

    # Fechas
    df['fechaPresentacion'] = df['fechaPresentacion'].apply(convert_to_timestamp)
    df['lastUpdated'] = df['lastUpdated'].apply(convert_date_to_array)
    
    columns_to_keep = list(mapeo_zgz.keys())
    df_filtered = df[columns_to_keep]
    # Renombrar las columnas
    df_minors_zgz = df_filtered.rename(columns=mapeo_zgz)
    print(f"Hay un total de {len(df_minors_zgz)} menores de ZARAGOZA")
    df_minors_zgz['id'] = 'zaragoza_opendata'
    
    df_combined_minors_zgz = pd.concat([df_minors, df_minors_zgz], ignore_index=True)

    df_combined_minors_zgz['ContractFolderStatus.TenderingProcess.TenderSubmissionDeadlinePeriod.EndDate'] = df_combined_minors_zgz['ContractFolderStatus.TenderingProcess.TenderSubmissionDeadlinePeriod.EndDate'].astype(str)
    df_final = df_combined_minors_zgz.map(ensure_array)
    
    return df_final

def unify_colname(col):
    return ".".join([el for el in col if el])

# Auxiliar functions for Madrid's Open Data integration
def replace_names_by_codes(df, column_name):
    tipo_contrato_codigo = {
        "Suministros": 1.0,
        "SUMINISTRO": 1.0,
        "Servicios": 2.0,
        "SERVICIOS": 2.0,
        "Concesión de Servicios": 2.0,
        "Obras": 3.0,
        "OBRAS": 3.0,
        "Gestión de Servicios Públicos": 21.0,
        "Concesión de Obras Públicas": 31.0,
        "Colaboración entre el sector público y el sector privado": 40.0,
        "Administrativo especial": 7.0,
        "Privado": 8.0,
        "Patrimonial": 50.0,
        "Otros": 999.0,
        "OTROS": 999.0
    }
    if column_name in df.columns:
        df[column_name] = df[column_name].map(tipo_contrato_codigo).astype('float64')
    return df

def convertir_importe_a_float(df, column_name):
    def limpiar_valor_importe(valor):
        if pd.isna(valor):
            return valor
        # Eliminar el símbolo de euro y cualquier otro carácter no numérico, excepto la coma (se aplica sobre columna str)
        valor_limpio = valor.replace('€', '').replace('.', '').replace(',', '.')
        try:
            return float(valor_limpio)
        except ValueError:
            return pd.NA

    if column_name in df.columns:
        df[column_name] = df[column_name].apply(limpiar_valor_importe).astype('float64')

    return df

def convert_to_float_array(x):
    return np.array([format(float(x), '.1f')], dtype=object)

def convert_to_float64(s):
    try:
        if pd.isna(s):
            return np.nan
        if isinstance(s, str):
            # Reemplazar comas por puntos para manejar formatos numéricos con comas como separador decimal
            s = s.replace(',', '.')
            return float(s)
        elif isinstance(s, (int, float)):
            return float(s)
        else:
            # Manejar otros tipos de datos si es necesario
            return np.nan
    except ValueError as e:
        print(f"Error al convertir '{s}' a float64: {e}")
        return np.nan
    
# Función para convertir valores 'SI'/'NO' en un array de numpy con 'true'/'false'
def convert_pyme_to_boolean_array(value):
    if value == 'SI':
        return np.array(['true'], dtype=object)  
    elif value == 'NO':
        return np.array(['false'], dtype=object)  
    else:
        return np.array(['undefined'], dtype=object)  

def convertir_timestamp_a_array(timestamp):
    # Convertir el timestamp a formato de fecha 'YYYY-MM-DD'
    fecha_formateada = timestamp.strftime('%Y-%m-%d')
    resultado_array = np.array([fecha_formateada], dtype=object)
    return resultado_array

def eliminar_corchetes(cadena):
    if isinstance(cadena, str):
        # Eliminar todos los corchetes
        return re.sub(r'[\[\]]', '', cadena).strip("'").strip('"')
    return cadena


def process_madrid(df_minors_base, input_dir):
    """
    Process the Madrid's Open Data DataFrames to extract and transform the relevant fields.
    Return: df with the relevant fields concatenated with the df_minors DataFrame.
    """
    # Read the Excel files
    try:
        df_mad_24 = pd.read_excel(os.path.join(input_dir, '300253-21-contratos-actividad-menores.xlsx'), sheet_name=0, parse_dates=True)
        df_mad_23 = pd.read_excel(os.path.join(input_dir, '300253-19-contratos-actividad-menores.xlsx'), sheet_name=0, skiprows=5, usecols="B:R", parse_dates=True)
        df_mad_22 = pd.read_excel(os.path.join(input_dir, '300253-17-contratos-actividad-menores.xlsx'), sheet_name=0, parse_dates=True)
        df_mad_21_15 = pd.read_excel(os.path.join(input_dir, '300253-15-contratos-actividad-menores.xlsx'), sheet_name=0, parse_dates=True)

        df_mad_21_13 = pd.read_excel(os.path.join(input_dir, '300253-13-contratos-actividad-menores.xlsx'), sheet_name=0, parse_dates=True)

        df_mad_20 = pd.read_excel(os.path.join(input_dir, '300253-11-contratos-actividad-menores.xlsx'), sheet_name=0, parse_dates=True)
        df_mad_19 = pd.read_excel(os.path.join(input_dir, '300253-9-contratos-actividad-menores.xlsx'), sheet_name=0, parse_dates=True)
        df_mad_18 = pd.read_excel(os.path.join(input_dir, '300253-1-contratos-actividad-menores.xlsx'), sheet_name=0, parse_dates=True)
    except Exception as e:
        print(f"Error al leer los archivos de Madrid: {e}")
        return None

    # Diccionarios de mapeo diferentes para cada subconjunto de datos
    mapeo_24_23_22_21_15 = {
        'N. DE EXPEDIENTE': 'ContractFolderStatus.ContractFolderID',
        'ORGANO DE CONTRATACION': 'ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name',
        'OBJETO DEL CONTRATO': 'ContractFolderStatus.ProcurementProject.Name',
        'TIPO DE CONTRATO': 'ContractFolderStatus.ProcurementProject.TypeCode',
        'IMPORTE LICITACION IVA INC.': 'ContractFolderStatus.ProcurementProject.BudgetAmount.TotalAmount',
        'N. LICITADORES PARTICIPANTES': 'ContractFolderStatus.TenderResult.ReceivedTenderQuantity',
        'NIF ADJUDICATARIO': 'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.ID',
        'RAZON SOCIAL ADJUDICATARIO': 'ContractFolderStatus.TenderResult.WinningParty.PartyName.Name',
        'PYME': 'ContractFolderStatus.TenderResult.SMEAwardedIndicator',
        'IMPORTE ADJUDICACION IVA INC.': 'ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.PayableAmount',
        'FECHA DE ADJUDICACION': 'ContractFolderStatus.TenderResult.AwardDate',
        'PLAZO': 'ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'
    }

    mapeo_21_13 = {
        'EXPEDIENTE': 'ContractFolderStatus.ContractFolderID',
        'ORG_CONTRATACIÓN': 'ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name',
        'OBJETO': 'ContractFolderStatus.ProcurementProject.Name',
        'TIPO_CONTRATO': 'ContractFolderStatus.ProcurementProject.TypeCode',
        'CIF': 'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.ID',
        'RAZÓN_SOCIAL': 'ContractFolderStatus.TenderResult.WinningParty.PartyName.Name',
        'IMPORTE': 'ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.PayableAmount',
        'F_APROBACIÓN':'ContractFolderStatus.TenderResult.AwardDate',
        'PLAZO': 'ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'
    }

    mapeo_20_19_18 = {
        'NUMERO EXPEDIENTE': 'ContractFolderStatus.ContractFolderID',
        'ORG.CONTRATACION': 'ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name',
        'OBJETO DEL CONTRATO': 'ContractFolderStatus.ProcurementProject.Name',
        'TIPO DE CONTRATO': 'ContractFolderStatus.ProcurementProject.TypeCode',
        'CONTRATISTA': 'ContractFolderStatus.TenderResult.WinningParty.PartyName.Name',
        'RAZON SOCIAL ADJUDICATARIO': 'ContractFolderStatus.TenderResult.WinningParty.PartyName.Name',
        'N.I.F': 'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.ID',
        'IMPORTE': 'ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.PayableAmount',
        'F_APROBACIÓN': 'ContractFolderStatus.TenderResult.AwardDate',
        'PLAZO': 'ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'
    }

    # Procesamiento de los DataFrames
    # Bloque 1
    dfs_bloque1 = [df_mad_24, df_mad_23, df_mad_22, df_mad_21_15]
    df_bloque1 = pd.concat(dfs_bloque1, ignore_index=True)

    df_bloque1 = replace_names_by_codes(df_bloque1, 'TIPO DE CONTRATO')
    df_bloque1 = convertir_importe_a_float(df_bloque1, 'IMPORTE LICITACION IVA INC.')
    df_bloque1['N. DE EXPEDIENTE'] = df_bloque1['N. DE EXPEDIENTE'].apply(convert_to_object_array)
    df_bloque1['N. LICITADORES PARTICIPANTES'] = df_bloque1['N. LICITADORES PARTICIPANTES'].apply(convert_to_float_array)
    df_bloque1['NIF ADJUDICATARIO'] = df_bloque1['NIF ADJUDICATARIO'].apply(convert_to_object_array)
    df_bloque1['RAZON SOCIAL ADJUDICATARIO'] = df_bloque1['RAZON SOCIAL ADJUDICATARIO'].apply(convert_to_object_array)
    df_bloque1['PLAZO'] = df_bloque1['PLAZO'].apply(convert_to_float64)
    df_bloque1['PYME'] = df_bloque1['PYME'].apply(convert_pyme_to_boolean_array)
    df_bloque1 = convertir_importe_a_float(df_bloque1, 'IMPORTE ADJUDICACION IVA INC.')
    df_bloque1['IMPORTE ADJUDICACION IVA INC.'] = df_bloque1['IMPORTE ADJUDICACION IVA INC.'].apply(convert_to_float_array)
    df_bloque1['FECHA DE ADJUDICACION'] = df_bloque1['FECHA DE ADJUDICACION'].apply(convert_date_to_array)

    # Bloque 2
    df_bloque2 = df_mad_21_13.copy()
    df_bloque2 = replace_names_by_codes(df_bloque2, 'TIPO_CONTRATO')
    df_bloque2['CIF'] = df_bloque2['CIF'].apply(convert_to_object_array)
    df_bloque2['RAZÓN_SOCIAL'] = df_bloque2['RAZÓN_SOCIAL'].apply(convert_to_object_array)
    df_bloque2['IMPORTE'] = df_bloque2['IMPORTE'].apply(convert_to_float_array)
    df_bloque2['F_APROBACIÓN'] = df_bloque2['F_APROBACIÓN'].apply(convertir_timestamp_a_array)

    # Bloque 3
    dfs_bloque3 = [df_mad_20, df_mad_19, df_mad_18]
    df_bloque3 = pd.concat(dfs_bloque3, ignore_index=True)
    df_bloque3 = replace_names_by_codes(df_bloque3, 'TIPO DE CONTRATO')
    df_bloque3['CONTRATISTA'] = df_bloque3['CONTRATISTA'].apply(convert_to_object_array)
    df_bloque3['N.I.F'] = df_bloque3['N.I.F'].apply(convert_to_object_array)
    df_bloque3['IMPORTE'] = df_bloque3['IMPORTE'].apply(convert_to_float_array)
    df_bloque3['FECHA APROBACION'] = df_bloque3['FECHA APROBACION'].apply(convertir_timestamp_a_array)

    # Renombrar las columnas de los dataframes según los diccionarios de mapeo
    df_bloque1.rename(columns=mapeo_24_23_22_21_15, inplace=True)
    df_bloque2.rename(columns=mapeo_21_13, inplace=True)
    df_bloque3.rename(columns=mapeo_20_19_18, inplace=True)

    df_bloque1.drop(columns=['N. DE REGISTRO DE CONTRATO','CENTRO - SECCION','N. DE INVITACIONES CURSADAS','INVITADOS A PRESENTAR OFERTA','FECHA DE INSCRIPCION'], inplace=True, errors='ignore')
    df_bloque2.drop(columns=['CONTRATO','SECCIÓN','F_INSCRIPCION'], inplace=True, errors='ignore')
    df_bloque3.drop(columns=['NºRECON','SECCION ', 'FECHA APROBACION', 'FCH.COMUNIC.REG'], inplace=True, errors='ignore')

    df_bloque1['id'] = 'madrid_opendata'
    df_bloque2['id'] = 'madrid_opendata'
    df_bloque3['id'] = 'madrid_opendata'

    # Concatenar todos los DataFrames
    dataframes = [df_bloque1, df_bloque2, df_bloque3]
    df_concatenado = df_minors_base.copy().reset_index(drop=True)

    for df in dataframes:
        df.reset_index(drop=True, inplace=True) 
        df_concatenado = pd.concat([df_concatenado, df], ignore_index=True)
    
    df_concatenado['ContractFolderStatus.ProcurementProject.BudgetAmount.TotalAmount'] = df_concatenado['ContractFolderStatus.ProcurementProject.BudgetAmount.TotalAmount'].astype(str)
    df_concatenado['ContractFolderStatus.ProcurementProject.BudgetAmount.TotalAmount'] = df_concatenado['ContractFolderStatus.ProcurementProject.BudgetAmount.TotalAmount'].apply(eliminar_corchetes)
    df_concatenado['ContractFolderStatus.ProcurementProject.BudgetAmount.TotalAmount'] = df_concatenado['ContractFolderStatus.ProcurementProject.BudgetAmount.TotalAmount'].apply(convert_to_object_array)

    df_concatenado['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'] = df_concatenado['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'].astype(str)
    df_concatenado['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'] = df_concatenado['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'].apply(eliminar_corchetes)
    df_concatenado['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'] = df_concatenado['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'].apply(convert_to_object_array)
    
    df_concatenado['ContractFolderStatus.ProcurementProject.TypeCode'] = \
        df_concatenado['ContractFolderStatus.ProcurementProject.TypeCode'].astype(str)
        
    df_concatenado['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'] = \
        df_concatenado['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'].astype(str)
    
    cols = set(df_bloque1.columns.tolist() + df_bloque2.columns.tolist() + df_bloque3.columns.tolist())
    cols = list(cols)

    for col in cols:
        if col in df_concatenado.columns:  
            df_concatenado[col] = df_concatenado[col].apply(convert_to_object_array)
        
    #Guardado intermedio, luego comentar en versión final
    df_concatenado.to_parquet('/export/usuarios_ml4ds/cggamella/sproc/DESCARGAS/zgz_y_madrid.parquet')
            
    return df_concatenado
        
        
# Auxiliar functions for Gencat's Open Data integration
def crear_indice_unico(row):
    # Primeros 50 caracteres de 'title'
    inicio_title = row['title'][:50]
    # ContractFolderStatus.ContractFolderID
    contract_folder_id = row['ContractFolderStatus.ContractFolderID']
    # ContractFolderStatus.ProcurementProject.RealizedLocation.CountrySubentityCode
    country_subentity_code = row['ContractFolderStatus.ProcurementProject.RealizedLocation.CountrySubentityCode']
    nom = row['ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name'][:50] if pd.notnull(row['ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name']) else ""
    # Concatenación de los componentes con el símbolo '&'
    indice_unico = f"{inicio_title}&{contract_folder_id}&{country_subentity_code}&{nom}"
    return indice_unico

def crear_indice_unico_gencat(row):
    # Primeros 50 caracteres de 'objecte_contracte'
    inicio_objecte_contracte = row['objecte_contracte'][:50] if pd.notnull(row['objecte_contracte']) else ""
    # 'codi_expedient' completo
    codi_expedient = row['codi_expedient'] 
    # 'codi_nuts' para el componente final del índice
    codi_nuts = row['codi_nuts']
    # 'nom_organ' para el nombre
    nom_organ = row['nom_organ'][:50] if pd.notnull(row['nom_organ']) else ""
    
    # Concatenación de los componentes con el símbolo '&'
    indice_unico = f"{inicio_objecte_contracte}&{codi_expedient}&{codi_nuts}&{nom_organ}"
    return indice_unico

def estadisticas_con_info(df):
    estadisticas = {}
    
    for columna in df.columns:
        # Contar valores NaN y None en la columna
        num_nan = df[columna].isna().sum() 
        # Contar cadenas vacías en la columna
        num_vacios = (df[columna].apply(lambda x: isinstance(x, str) and x.strip() == '')).sum()
        # Calcular el total de valores
        total_valores = df[columna].shape[0]
        num_con_info = total_valores - num_nan - num_vacios
        
        estadisticas[columna] = {
            'Con información': num_con_info,
            'Total valores': total_valores,
            'Porcentaje con información': (num_con_info / total_valores) * 100,
        }
    estadisticas_df = pd.DataFrame(estadisticas).T  
    
    return estadisticas_df

def flatten_value(x):
    if isinstance(x, str) and x.startswith("[") and x.endswith("]"):
        try:
            # Convierte la cadena en una lista real usando ast.literal_eval
            x = ast.literal_eval(x)
        except (ValueError, SyntaxError):
            pass  
    if isinstance(x, (list, np.ndarray)) and len(x) > 0:
        return float(x[0])  
    try:
        return float(x)  
    except ValueError:
        return None
    
def reemplazar_valores_vacios(df, mapeo_gencat):
    """
    Reemplazar valores vacíos (NaN, None o '') en las columnas de destino
    con valores de las columnas de origen según el mapeo proporcionado.
    Si la columna de origen es un array, se utilizarán todos los valores.
    Además, genera estadísticas de cuántas sustituciones se han realizado por columna.
    """
    # Dict para datos de sustituciones
    sustituciones = {col_destino: 0 for col_destino in mapeo_gencat.values()}
    
    # Recorre cada fila del DataFrame
    for index, row in df.iterrows():
        for col_origen, col_destino in mapeo_gencat.items():
            if col_origen not in df.columns or col_destino not in df.columns:
                continue
            
            # Verifica si el valor en la columna de destino es NaN, None o vacío
            valor_destino = row[col_destino]
            valor_origen = row[col_origen]

            # Verificación adicional para arrays que contengan 'nan'
            if isinstance(valor_origen, np.ndarray):
                if valor_origen.size == 1 and valor_origen[0] == 'nan':
                    continue
                elif valor_origen.size > 0 and all(pd.notna(valor_origen)):
                    valor_origen = ', '.join(map(str, valor_origen))  

            # Convertir la columna de destino a tipo 'object' si no lo es
            if not pd.api.types.is_object_dtype(df[col_destino]):
                df[col_destino] = df[col_destino].astype(object)

            # Si el valor de destino es NaN, None o vacío, y el valor de origen es válido
            if (pd.isna(valor_destino) or valor_destino == '') and pd.notna(valor_origen):
                df.at[index, col_destino] = valor_origen
                sustituciones[col_destino] += 1  
                #print(f"Fila {index} | Columna '{col_destino}' | Valor original: {valor_destino} | Valor de reemplazo: {valor_origen}")

    print("\nEstadísticas de Sustituciones:")
    for col, count in sustituciones.items():
        if count > 0:
            print(f"Columna '{col}': {count} sustituciones realizadas.")

    return df

def analyze_duration(duration_str):
    """
    Función para convertir la duración a días.
    Maneja distintos formatos.
    """
    if 'anys' in duration_str or 'mesos' in duration_str or 'dies' in duration_str:
        # Manejar el formato de duración como 'X anys Y mesos Z dies'
        parts = duration_str.split()
        years, months, days = 0, 0, 0

        try:
            for i, part in enumerate(parts):
                if part == 'anys':
                    years = int(parts[i-1])
                elif part == 'mesos':
                    months = int(parts[i-1])
                elif part == 'dies':
                    days = int(parts[i-1])
        except ValueError:
            pass  
        
        total_days = years * 365 + months * 30 + days
        return total_days

    elif ' a ' in duration_str:
        # Manejar el formato de rango de fechas 'dd/mm/yyyy a dd/mm/yyyy'
        try:
            start_date_str, end_date_str = duration_str.split(' a ')
            start_date = datetime.strptime(start_date_str.strip(), '%d/%m/%Y')
            end_date = datetime.strptime(end_date_str.strip(), '%d/%m/%Y')
            return (end_date - start_date).days
        except ValueError:
            return None
        
    elif '-' in duration_str and ':' in duration_str:
            # Manejar formato de fecha 'YYYY-MM-DD HH:MM:SS'
            try:
                date_time_obj = datetime.strptime(duration_str.strip(), '%Y-%m-%d %H:%M:%S')
                # Si solo es una fecha (no un rango), devolver 0 días
                return 0
            except ValueError:
                return None

    return None

def procesar_datos_json(df_input, max_retries=3, delay_seconds=2):
    df = df_input.copy()
    failed_urls = []
    
    for index, row in df.iterrows():
        # Verificar si la columna 'url_json_licitacio' es válida
        if pd.isna(row['url_json_licitacio']) or row['url_json_licitacio'] == "":
            continue      
        try:
            entrada_dict = ast.literal_eval(row['url_json_licitacio'])
            if 'url' not in entrada_dict or not isinstance(entrada_dict['url'], str):
                print("La entrada no contiene una 'url' válida o no es una cadena")
                continue
            
            url = entrada_dict['url']
            success = False
            attempts = 0

            while attempts < max_retries and not success:
                try:
                    response = requests.get(url, timeout=10)
                    attempts += 1

                    if response.status_code == 200:
                        datos = response.json()
                        try:
                            if datos['publicacio']['dadesPublicacio']['plecsDeClausulesAdministratives']['ca']:
                                df.loc[index, 'ContractFolderStatus.LegalDocumentReference.ID'] = ', '.join([item.get('titol', '') for item in datos['publicacio']['dadesPublicacio']['plecsDeClausulesAdministratives']['ca']])
                                df.loc[index, 'ContractFolderStatus.LegalDocumentReference.Attachment.ExternalReference.URI'] = ', '.join([item.get('path', '') for item in datos['publicacio']['dadesPublicacio']['plecsDeClausulesAdministratives']['ca']])
                            else:
                                df.loc[index, 'ContractFolderStatus.LegalDocumentReference.ID'] = None
                                df.loc[index, 'ContractFolderStatus.LegalDocumentReference.Attachment.ExternalReference.URI'] = None

                            # Concatenar todos los títulos y paths de 'plecsDePrescripcionsTecniques'
                            if datos['publicacio']['dadesPublicacio']['plecsDePrescripcionsTecniques']['ca']:
                                df.loc[index, 'ContractFolderStatus.TechnicalDocumentReference.ID'] = ', '.join([item.get('titol', '') for item in datos['publicacio']['dadesPublicacio']['plecsDePrescripcionsTecniques']['ca']])
                                df.loc[index, 'ContractFolderStatus.TechnicalDocumentReference.Attachment.ExternalReference.URI'] = ', '.join([item.get('path', '') for item in datos['publicacio']['dadesPublicacio']['plecsDePrescripcionsTecniques']['ca']])
                            else:
                                df.loc[index, 'ContractFolderStatus.TechnicalDocumentReference.ID'] = None
                                df.loc[index, 'ContractFolderStatus.TechnicalDocumentReference.Attachment.ExternalReference.URI'] = None

                            if df.loc[index, 'codi_dir3'] in [None, '', 'nan', 'NaN']:
                                print("La columna contiene valores NaN, None, o vacíos-> Buscando en el JSON")
                                df.loc[index, 'ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID'] = datos['organ']['organContractacioId']      

                            df.loc[index, 'ContractFolderStatus.ProcurementProject.ContractExtension.OptionsDescription'] = str(datos['publicacio']['dadesPublicacio'].get('preveuenProrroguesAlsPlecs', ''))

                            data_solvenciesEconomiques = []
                            data_solvenciesTecniques = []
                            data_condicioExecucio = []
                            data_criterisAdjudicacio = []

                            # Iterar sobre todos los lotes
                            for lot in datos['publicacio']['dadesPublicacioLot']:
                                # Obtener solvenciesEconomiques del lote actual
                                solvencies_economiques = lot.get('solvenciesEconomiques', [])
                                for item in solvencies_economiques:
                                    data = {
                                    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.FinancialEvaluationCriteria.EvaluationCriteriaTypeCode': item.get('criteriSolvencia', {}).get('ca', ''),
                                    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.FinancialEvaluationCriteria.Description': item.get('descripcioCriteriSolvencia', {}).get('ca', ''),
                                    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.FinancialEvaluationCriteria.ThresholdQuantity': item.get('valorMinimExigit', {}).get('ca', '')
                                    #'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.FinancialEvaluationCriteria.ThresholdQuantity': item.get('valorMinimExigit', {})
                                    }
                                    data_solvenciesEconomiques.append(data)

                                # Solo ejecutar el siguiente bloque si data_solvenciesEconomiques no está vacío
                                if data_solvenciesEconomiques:
                                    for key in data_solvenciesEconomiques[0].keys():
                                        # Concatenar valores para cada clave en la lista de diccionarios
                                        concatenated_values = ', '.join([d[key] if d[key] else 'None' for d in data_solvenciesEconomiques])
                                        df.loc[index, key] = concatenated_values
                                else:
                                    print("No hay datos en data_solvenciesEconomiques para el índice", index)

                                # Obtener solvenciesTecniques del lote actual
                                solvencies_tecniques = lot.get('solvenciesTecniques', [])
                                for item in solvencies_tecniques:
                                    data = {
                                        'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.TechnicalEvaluationCriteria.EvaluationCriteriaTypeCode': item.get('criteriSolvencia', {}).get('ca', ''),
                                        'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.TechnicalEvaluationCriteria.Description': item.get('descripcioCriteriSolvencia', {}).get('ca', ''),
                                        'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.TechnicalEvaluationCriteria.ThresholdQuantity': item.get('valorMinimExigit', {}).get('ca', '')
                                    }
                                    data_solvenciesTecniques.append(data)

                                # Para data_solvenciesTecniques
                                if data_solvenciesTecniques:

                                    for key in data_solvenciesTecniques[0].keys():
                                        concatenated_values = ', '.join([str(d[key]) if d[key] else 'None' for d in data_solvenciesTecniques])
                                        df.loc[index, key] = concatenated_values
                                else:
                                    print("No hay datos en data_solvenciesTecniques para el índice", index)

                                # Obtener condicionsExecucio del lote actual
                                condicio_execucio = lot.get('condicionsExecucio')

                                # Iterar sobre todos los lotes de 'solvenciesTecniques'
                                for item in condicio_execucio:
                                    data = {
                                        'ContractFolderStatus.TenderingTerms.ContractExecutionRequirement.ExecutionRequirementCode': item.get('tipusCondicioExecucio', {}).get('id', ''),
                                        'ContractFolderStatus.TenderingTerms.ContractExecutionRequirement.Name': item.get('tipusCondicioExecucio', {}).get('ca', ''),
                                        'ContractFolderStatus.TenderingTerms.ContractExecutionRequirement.Description': item.get('descripcio', {}).get('ca', '')
                                    }
                                    data_condicioExecucio.append(data)

                                # Para data_condicioExecucio
                                if data_condicioExecucio:
                                    for key in data_condicioExecucio[0].keys():
                                        concatenated_values = ', '.join([str(d[key]) if d[key] else 'None' for d in data_condicioExecucio])
                                        df.loc[index, key] = concatenated_values
                                else:
                                    print("No hay datos en data_condicioExecucio para el índice", index)

                                ## XXXXXXXXX
                                # Obtener criterisAdjudicacio del lote actual
                                criteris_adjudicacio = lot.get('criterisAdjudicacio')

                                # Iterar sobre todos los lotes de 'solvenciesTecniques'
                                for item in criteris_adjudicacio:
                                    data = {
                                        'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.AwardingTerms.AwardingCriteria.AwardingCriteriaTypeCode': item.get('tipusCriteri', {}).get('ca', ''),
                                        'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.AwardingTerms.AwardingCriteria.AwardingCriteriaSubTypeCode': item.get('tipusAvaluacio', {}).get('ca', ''),
                                        'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.AwardingTerms.AwardingCriteria.Description': item.get('descripcioCriteri', {}).get('ca', ''),
                                        'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.AwardingTerms.AwardingCriteria.WeightNumeric': item.get('ponderacio', {})
                                    }
                                    data_criterisAdjudicacio.append(data)

                                # Para data_criterisAdjudicacio
                                if data_criterisAdjudicacio:
                                    for key in data_criterisAdjudicacio[0].keys():
                                        concatenated_values = ', '.join([str(d[key]) if d[key] else 'None' for d in data_criterisAdjudicacio])
                                        df.loc[index, key] = concatenated_values
                                else:
                                    print("No hay datos en data_criterisAdjudicacio para el índice", index)

                            success = True
                            
                        except KeyError as e:
                            print(f"KeyError encontrado: {e}")
                            failed_urls.append({'url': url, 'error': str(e)})
                            df.loc[index, 'error'] = str(e)
                            success = True
                    else:
                        print(f"Respuesta HTTP no exitosa (Código {response.status_code}), reintentando...")
                        time.sleep(delay_seconds)

                except requests.exceptions.RequestException as e:
                    print(f"Error en el intento {attempts}: {e}")
                    if attempts >= max_retries:
                        df.loc[index, 'url_json_licitacio'] = 'Error tras varios intentos: ' + str(e)
                    time.sleep(delay_seconds)

        except (ValueError, SyntaxError) as e:
            print(f"Error al evaluar la cadena JSON en el índice {index}: {e}")
            df.loc[index, 'url_json_licitacio'] = 'Error al evaluar JSON'
    
    df_failed = pd.DataFrame(failed_urls)
    df_failed.to_csv('failed_urls.csv', index=False)
    
    return df

def agrupar_dataframe(df, cols_to_group):
    # Crear una copia del DataFrame para no modificar el original
    df_resultado = df.copy()

    # Agrupar y transformar las columnas especificadas
    grouped = df_resultado.groupby('indice_unico')[cols_to_group].transform(lambda x: [list(x)]*len(x))
    df_resultado[cols_to_group] = grouped

    return df_resultado

mapeo_gencat_mejora_outsiders = {
    'enllac_publicacio': 'link',
    'codi_expedient': 'ContractFolderStatus.ContractFolderID',
    'objecte_contracte': 'title',
    'codi_cpv': 'ContractFolderStatus.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode',
    'pressupost_licitacio_sense': 'ContractFolderStatus.ProcurementProject.BudgetAmount.TaxExclusiveAmount', 
    'codi_dir3': 'ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID',
    
    'nom_organ': 'ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name',  
    'termini_presentacio_ofertes': 'ContractFolderStatus.TenderingProcess.TenderSubmissionDeadlinePeriod.EndDate',
    'durada_contracte': 'ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure',
    
    'tipus_contracte': 'ContractFolderStatus.ProcurementProject.TypeCode',
    'procediment': 'ContractFolderStatus.TenderingProcess.ProcedureCode',
    'tipus_tramitacio': 'ContractFolderStatus.TenderingProcess.UrgencyCode',
    'tipus_financament': 'ContractFolderStatus.TenderingTerms.FundingProgramCode',
    
    'numero_lot': 'ContractFolderStatus.ProcurementProjectLot.ID',
    'descripcio_lot': 'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.Name',
    #'pressupost_licitacio_sense': 'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.BudgetAmount.TaxExclusiveAmount',
    'resultat': 'ContractFolderStatus.TenderResult.AwardedTenderedProject.ProcurementProjectLotID',
    'ofertes_rebudes': 'ContractFolderStatus.TenderResult.ReceivedTenderQuantity',
    'identificacio_adjudicatari': 'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.ID',
    'tipus_identificacio': 'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.IDschemeName',
    'denominacio_adjudicatari': 'ContractFolderStatus.TenderResult.WinningParty.PartyName.Name',
    'import_adjudicacio_sense': 'ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'
    #'tipus_empresa': 'ContractFolderStatus.TenderResult.SMEAwardedIndicator', NO SE USA YA QUE NO EXISTE EN PLACE     
}

columns_keep = [
    'link',
    'ContractFolderStatus.ContractFolderID',
    'title',
    'ContractFolderStatus.LegalDocumentReference.ID',
    'ContractFolderStatus.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode',
    #'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode',
    
    'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.BudgetAmount.TaxExclusiveAmount',
    'ContractFolderStatus.LegalDocumentReference.Attachment.ExternalReference.URI',
    'ContractFolderStatus.TechnicalDocumentReference.ID',
    'ContractFolderStatus.TechnicalDocumentReference.Attachment.ExternalReference.URI',
    'ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID',
    'ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name',
    
    'ContractFolderStatus.TenderingProcess.TenderSubmissionDeadlinePeriod.EndDate',
    'ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure',
    
    'ContractFolderStatus.ProcurementProject.ContractExtension.OptionsDescription',
    'ContractFolderStatus.ProcurementProject.TypeCode',
    'ContractFolderStatus.TenderingProcess.ProcedureCode',
    'ContractFolderStatus.TenderingProcess.UrgencyCode',
    'ContractFolderStatus.TenderingTerms.FundingProgramCode',
   
    'ContractFolderStatus.ProcurementProjectLot.ID',
    'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.Name',
    #'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.BudgetAmount.TaxExclusiveAmount',
    #'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode',
     
    # Criterios de evaluación técnica por lotes
    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.TechnicalEvaluationCriteria.EvaluationCriteriaTypeCode',
    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.TechnicalEvaluationCriteria.Description',
    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.TechnicalEvaluationCriteria.ThresholdQuantity',
    
    # Criterios de evaluación económica por lotes
    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.FinancialEvaluationCriteria.EvaluationCriteriaTypeCode',
    #'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.FinancialEvaluationCriteria.Description',
    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.TendererQualificationRequest.FinancialEvaluationCriteria.ThresholdQuantity',
    
    # Condiciones especiales
    'ContractFolderStatus.TenderingTerms.ContractExecutionRequirement.ExecutionRequirementCode',
    'ContractFolderStatus.TenderingTerms.ContractExecutionRequirement.Name',
    'ContractFolderStatus.TenderingTerms.ContractExecutionRequirement.Description',
    
    # Criterios de adjudicación por lotes
    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.AwardingTerms.AwardingCriteria.AwardingCriteriaTypeCode',
    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.AwardingTerms.AwardingCriteria.AwardingCriteriaSubTypeCode',
    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.AwardingTerms.AwardingCriteria.Description',
    'ContractFolderStatus.ProcurementProjectLot.TenderingTerms.AwardingTerms.AwardingCriteria.WeightNumeric',
    
    'ContractFolderStatus.TenderResult.AwardedTenderedProject.ProcurementProjectLotID',
    
    'ContractFolderStatus.TenderResult.ReceivedTenderQuantity',
    
    'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.ID',
    'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.IDschemeName',
    'ContractFolderStatus.TenderResult.WinningParty.PartyName.Name',
    'ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount',
    'ContractFolderStatus.TenderResult.SMEAwardedIndicator'
]

def process_gencat(df_minors_base, df_outsiders_base, input_dir):
    """
    Procesa opendata de Gencat y los integra con subconjuntos de contratos menores y outsiders de PLACE.
    """
    # Cargar datos de Gencat
    # El archivo CSV tiene 'catalunya' en el nombre y está en input_dir
    gencat_files = [f for f in os.listdir(input_dir) if 'catalunya' in f and f.endswith('.csv')]
    if not gencat_files:
        logging.info("No se encontró ningún archivo de Gencat con 'Catalunya' en el nombre. Añada y/o verifique el archivo CSV y vuelva a intentarlo.")
        return
    else:
        gencat_file = os.path.join(input_dir, gencat_files[0])
        logging.info(f"Cargando datos de Gencat desde {gencat_file}")
    
    df_gencat = pd.read_csv(gencat_file) 
    logging.info(f"Datos de Gencat cargados con {df_gencat.shape[0]} filas.")
    # Crear índice único en df_gencat
    df_gencat['indice_unico'] = df_gencat.apply(crear_indice_unico_gencat, axis=1)
    df_gencat['indice_unico'] = df_gencat['indice_unico'].str.lower()
    
    # Informativo: Estadísticas de valores en df_gencat
    num_urls_vacias = df_gencat['url_json_licitacio'].isna().sum()
    logging.info(f"Número de URLs vacías o NaN: {num_urls_vacias}")
    
    cols_to_group = ["identificacio_adjudicatari", "denominacio_adjudicatari", "import_adjudicacio_sense", "numero_lot",
                 "resultat", "pressupost_licitacio_sense", "descripcio_lot"]
    # Aplicar la función para agrupar el DataFrame tarda 9 mins
    logging.info("Agrupando el DataFrame de Gencat...")
    df_resultado = agrupar_dataframe(df_gencat, cols_to_group)
    
    # Eliminar duplicados en 'indice_unico' ya tengo la información agrupada
    df_resultado.drop_duplicates(subset=['indice_unico'], keep='first', inplace=True)
    
    # Procesar datos JSON en contratos_menores
    logging.info("Procesando datos JSON en contratos menores...")
    start_time = time.time()
    df_gencat_json = procesar_datos_json(df_resultado)
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"La función procesar_datos_json tarda {elapsed_time/60} mins en ejecutarse.")
    
    #df_gencat_json = df_resultado.copy() #Pruebas para no esperar a que extraiga info json
    #import pdb; pdb.set_trace()

    # SEPARAR LÓGICA DE MINORS Y OUTSIDERS
    contratos_menores = df_gencat_json[df_gencat_json['procediment'] == 'Contracte menor'].copy()
    contratos_outsiders = df_gencat_json[df_gencat_json['procediment'] != 'Contracte menor'].copy()
    logging.info(f"Total de filas en Gencat: {df_gencat_json.shape[0]}")
    logging.info(f"Contratos menores opendata: {contratos_menores.shape[0]} filas.")
    logging.info(f"Contratos outsiders opendata: {contratos_outsiders.shape[0]} filas.")
    #import pdb; pdb.set_trace()
    
    # Procesamiento de contratos menores
    logging.info("Procesando contratos menores de Gencat...")
    # Definir el mapeo de columnas para contratos menores
    mapeo_gencat = {
        'enllac_publicacio': 'link',
        'codi_expedient': 'ContractFolderStatus.ContractFolderID',
        'objecte_contracte': 'title',
        'codi_cpv': 'ContractFolderStatus.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode',
        'pressupost_licitacio_sense': 'ContractFolderStatus.ProcurementProject.BudgetAmount.TaxExclusiveAmount',
        'codi_dir3': 'ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID',
        'nom_organ': 'ContractFolderStatus.LocatedContractingParty.Party.PartyName.Name',
        'termini_presentacio_ofertes': 'ContractFolderStatus.TenderingProcess.TenderSubmissionDeadlinePeriod.EndDate',
        'durada_contracte': 'ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure',
        'tipus_contracte': 'ContractFolderStatus.ProcurementProject.TypeCode',
        'procediment': 'ContractFolderStatus.TenderingProcess.ProcedureCode',
        'tipus_tramitacio': 'ContractFolderStatus.TenderingProcess.UrgencyCode',
        'tipus_financament': 'ContractFolderStatus.TenderingTerms.FundingProgramCode',
        'numero_lot': 'ContractFolderStatus.ProcurementProjectLot.ID',
        'descripcio_lot': 'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.Name',
        'pressupost_licitacio_sense': 'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.BudgetAmount.TaxExclusiveAmount',
        'codi_cpv': 'ContractFolderStatus.ProcurementProjectLot.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode',
        'resultat': 'ContractFolderStatus.TenderResult.AwardedTenderedProject.ProcurementProjectLotID',
        'ofertes_rebudes': 'ContractFolderStatus.TenderResult.ReceivedTenderQuantity',
        'identificacio_adjudicatari': 'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.ID',
        'tipus_identificacio': 'ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.IDschemeName',
        'denominacio_adjudicatari': 'ContractFolderStatus.TenderResult.WinningParty.PartyName.Name',
        'import_adjudicacio_sense': 'ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount',
        'tipus_empresa': 'ContractFolderStatus.TenderResult.SMEAwardedIndicator',
    }

    # Renombrar columnas según el mapeo
    df_menores_ren = contratos_menores.rename(columns=mapeo_gencat)

    # Eliminar columnas innecesarias
    columns_to_drop = [
        'codi_ambit', 'nom_ambit', 'codi_organ', 'url_json_formalitzacio',
        'url_json_anulacio', 'data_publicacio_previ', 'no_admet_eina_licitacio',
        'data_publicacio_futura', 'url_json_previ', 'url_json_futura',
        'data_publicacio_consulta', 'url_json_cpm', 'data_publicacio_encarrec',
        'codi_ine10', 'fase_publicacio', 'denominacio',
        'data_formalitzacio_contracte', 'data_publicacio_anul',
        'racionalitzacio_contractacio', 'data_publicacio_avaluacio',
        'url_json_licitacio', 'url_json_avaluacio', 'es_agregada',
        'data_adjudicacio_contracte', 'data_publicacio_contracte',
        'eina_presentacio_electronica', 'altres_eines_licitacio',
        'url_json_agregada', 'codi_departament_ens', 'nom_departament_ens',
        'pressupost_licitacio_sense_1', 'pressupost_licitacio_amb',
        'pressupost_licitacio_amb_1', 'valor_estimat_expedient',
        'valor_estimat_contracte', 'codi_nuts', 'lloc_execucio',
        'data_publicacio_anunci', 'data_publicacio_formalitzacio',
        'import_adjudicacio_amb_iva', 'codi_unitat', 'nom_unitat',
        'data_publicacio_adjudicacio', 'url_json_adjudicacio'
    ]
    
    logging.info(f"Las columnas de gencat menores con rename son: {df_menores_ren.columns.tolist()}")
    df_menores_ren.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    df_menores_ren['link'] = df_menores_ren.apply(extraer_url, axis=1)
    df_menores_ren['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'] = df_menores_ren['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'].apply(
        lambda x: analyze_duration(x) if isinstance(x, str) else x
    )
    
    # Reemplazar códigos de procedimiento
    procedimiento_codigos = {
        "Ordinari":  1.0,
        "Ordinària": 1.0,
        "Urgent":    2.0,
        "Urgència":  2.0,
        "Emergència":3.0
    }
    def replace_procedure_code(value):
        return procedimiento_codigos.get(value, value)
    
    df_menores_ren['ContractFolderStatus.TenderingProcess.UrgencyCode'] = df_menores_ren['ContractFolderStatus.TenderingProcess.UrgencyCode'].apply(replace_procedure_code)
    #import pdb; pdb.set_trace()
    
    df_menores_ren['id'] = 'minors_gencat_opendata'
    df_menores_ren['id'] = df_menores_ren['id'].apply(convert_to_object_array)

    logging.info(f"Las cols de place de contratos menores son: {df_minors_base.columns.tolist()}")
    # Combinar con df_minors_base
    # Aqui esta vacío df_minors_base, pero debería ser el df de minors
    df_minors_combined = pd.concat([df_minors_base, df_menores_ren], ignore_index=True)
    logging.info(f"Total de contratos menores después de combinar: {df_minors_combined.shape[0]} filas.")
    
    # Unificar tipos de datos para guardar como parquet
    df_minors_combined['ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID'] = \
        df_minors_combined['ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.ProcurementProject.TypeCode'] = \
        df_minors_combined['ContractFolderStatus.ProcurementProject.TypeCode'].apply(substitute_type_code)    
    df_minors_combined['ContractFolderStatus.ProcurementProject.TypeCode'] = \
        df_minors_combined['ContractFolderStatus.ProcurementProject.TypeCode'].astype(str)
    df_minors_combined['ContractFolderStatus.ProcurementProject.TypeCode'] = \
        df_minors_combined['ContractFolderStatus.ProcurementProject.TypeCode'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'] = \
        df_minors_combined['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'].astype(str)
    df_minors_combined['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'] = \
        df_minors_combined['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.IDschemeName'] = \
        df_minors_combined['ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.IDschemeName'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'] = \
        df_minors_combined['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'].astype(str)
    df_minors_combined['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'] = \
        df_minors_combined['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'] = \
        df_minors_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'].apply(substitute_contract_value)
    df_minors_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'] = \
        df_minors_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'].astype(str)
    df_minors_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'] = \
        df_minors_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.ProcurementProjectLot.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode'] = \
        df_minors_combined['ContractFolderStatus.ProcurementProjectLot.ProcurementProject.RequiredCommodityClassification.ItemClassificationCode'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.TenderResult.SMEAwardedIndicator']= \
        df_minors_combined['ContractFolderStatus.TenderResult.SMEAwardedIndicator'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.TenderingTerms.FundingProgramCode']= \
        df_minors_combined['ContractFolderStatus.TenderingTerms.FundingProgramCode'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.ProcurementProjectLot.ID'] = \
        df_minors_combined['ContractFolderStatus.ProcurementProjectLot.ID'].apply(convert_to_object_array)
    df_minors_combined['ContractFolderStatus.ProcurementProjectLot.ID'] = \
        df_minors_combined['ContractFolderStatus.ProcurementProjectLot.ID'].apply(convert_to_ndarray)

    df_minors_combined['ContractFolderStatus.ProcurementProjectLot.ProcurementProject.BudgetAmount.TaxExclusiveAmount'] = \
        df_minors_combined['ContractFolderStatus.ProcurementProjectLot.ProcurementProject.BudgetAmount.TaxExclusiveAmount'].apply(convert_to_ndarray)

    df_minors_combined['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'] = \
        df_minors_combined['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'].apply(convert_to_ndarray)

    df_minors_combined['ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID'] = \
        df_minors_combined['ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID'].apply(convert_to_object_array)

    df_minors_combined['ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.IDschemeName'] = \
        df_minors_combined['ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.IDschemeName'].apply(convert_to_object_array)
    
    logging.info("Guardando datos de contratos MENORES procesados...")
    # Guardar el DataFrame de contratos menores procesados
    output_path_minors = os.path.join(input_dir, 'processed_gencat_minors.parquet')
    df_minors_combined.to_parquet(output_path_minors, index=False)
    logging.info(f"Datos de contratos menores de Gencat procesados y guardados en {output_path_minors}")
    #import pdb; pdb.set_trace()

    # Procesamiento de contratos OUTSIDERS XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    logging.info("Procesando contratos outsiders de Gencat...")
    print("*"*75)
    df_outsiders_base['indice_unico'] = df_outsiders_base.apply(crear_indice_unico, axis=1)
    df_outsiders_base['indice_unico'] = df_outsiders_base['indice_unico'].str.lower()
    #import pdb; pdb.set_trace()

    #direct_col_to_keep = df_outsiders_base.columns.tolist() # Guardar todos los datos de outsiders
    df_coincidencias_outsiders = pd.merge(df_outsiders_base, df_gencat_json, on='indice_unico', how='inner')
    logging.info(f"El total de coincidencias es: {len(df_coincidencias_outsiders)}")
    logging.info(f"El total de contratos outsiders en place es: {len(contratos_outsiders)}")
    #import pdb; pdb.set_trace()
    
    # Completando los datos de contratos outsiders con la información de Gencat
    logging.info("Completando los datos de contratos outsiders con la información de Gencat...")
    df_out_sustituido = reemplazar_valores_vacios(df_coincidencias_outsiders, mapeo_gencat_mejora_outsiders)
    df_out_sustituido['ContractFolderStatus.TenderingProcess.UrgencyCode'] = df_out_sustituido['ContractFolderStatus.TenderingProcess.UrgencyCode'].apply(replace_procedure_code)
    df_out_sustituido['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'] = df_out_sustituido['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'].apply(
        lambda x: analyze_duration(x) if isinstance(x, str) else x
    )
    
    cols_drop_extra = [
        'nom_organ',
        'codi_dir3',
        'codi_expedient',
        'tipus_contracte',
        'procediment',
        'objecte_contracte',
        'durada_contracte',
        'codi_cpv',
        'identificacio_adjudicatari',
        'denominacio_adjudicatari',
        'import_adjudicacio_sense',
        'enllac_publicacio',
        'numero_lot',
        'tipus_identificacio',
        'ofertes_rebudes',
        'resultat',
        'tipus_tramitacio',
        'termini_presentacio_ofertes',
        'pressupost_licitacio_sense',
        'descripcio_lot',
        'tipus_financament',
        'tipus_empresa'  
    ]
    # Quedarme columas de PLACE con la info ya enriquecida de Gencat, para eliminar cols de gencat
    #columns_to_keep = [col for col in columns_keep if col in df_out_sustituido.columns]
    cols_drop = columns_to_drop + cols_drop_extra
    df_out_sustituido.drop(columns=cols_drop, inplace=True, errors='ignore')

    logging.info(f"Las cols enriquecidas(gencat) de outsiders listas para concat: {df_out_sustituido.columns}")
    #import pdb; pdb.set_trace()

    ## REVISAR ESTO LOS INDICES PARA CONCATENAR
    # Combinar con df_outsiders_base 
    df_outsiders_base = df_outsiders_base.reset_index(drop=True)
    df_out_sustituido = df_out_sustituido.reset_index(drop=True)
    logging.info(f"Las columnas en df_outsiders_base son: {df_outsiders_base.columns.tolist()}")
    logging.info(f"Las columnas en df_out_sustituido son: {df_out_sustituido.columns.tolist()}")
    df_outsiders_combined = pd.concat([df_outsiders_base, df_out_sustituido], ignore_index=True)
    logging.info(f"Añadir contratos outsiders enriquecidos (coincidentes): {df_outsiders_combined.shape[0]} filas.")
    #import pdb; pdb.set_trace()
    
    df_outsiders_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'] = \
        df_outsiders_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'].apply(substitute_contract_value)
    df_outsiders_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'] = \
        df_outsiders_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'].astype(str)
    df_outsiders_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'] = \
        df_outsiders_combined['ContractFolderStatus.TenderingProcess.ProcedureCode'].apply(convert_to_object_array)
        
    logging.info(f"Las COLUMAS SON: {df_outsiders_combined.columns.tolist()}")
    #import pdb; pdb.set_trace()
    
    # Guardar el DataFrame de contratos outsiders procesados
    output_path_outsiders = os.path.join(input_dir, 'processed_gencat_outsiders_enriquecidos.parquet')
    df_outsiders_combined.to_parquet(output_path_outsiders, index=False)
    logging.info(f"Datos de contratos outsiders enriquecidos Gencat procesados y guardados en {output_path_outsiders}")
    #import pdb; pdb.set_trace()
    
    logging.info("Integrando contratos outsiders no coincidentes...")
    # Filtrar los contratos outsiders que no matchean
    indices_no_match = ~contratos_outsiders['indice_unico'].isin(df_coincidencias_outsiders['indice_unico'])
    df_out_no_coincidentes = contratos_outsiders[indices_no_match]
    df_out_no_coincidentes = df_out_no_coincidentes.rename(columns=mapeo_gencat)
    #import pdb; pdb.set_trace()
    
    df_outsiders_all = pd.concat([df_outsiders_combined, df_out_no_coincidentes], ignore_index=True)
    print(f"Total de contratos outsiders integrados: {df_outsiders_all.shape[0]} filas.")
    
    df_outsiders_all['ContractFolderStatus.TenderingProcess.ProcedureCode'] = \
        df_outsiders_all['ContractFolderStatus.TenderingProcess.ProcedureCode'].apply(substitute_contract_value)
    df_outsiders_all['ContractFolderStatus.TenderingProcess.ProcedureCode'] = \
        df_outsiders_all['ContractFolderStatus.TenderingProcess.ProcedureCode'].astype(str)
    df_outsiders_all['ContractFolderStatus.TenderingProcess.ProcedureCode'] = \
        df_outsiders_all['ContractFolderStatus.TenderingProcess.ProcedureCode'].apply(convert_to_object_array)
        
    df_outsiders_all['ContractFolderStatus.ProcurementProject.TypeCode'] = \
        df_outsiders_all['ContractFolderStatus.ProcurementProject.TypeCode'].apply(substitute_type_code)
    df_outsiders_all['ContractFolderStatus.ProcurementProject.TypeCode'] = \
        df_outsiders_all['ContractFolderStatus.ProcurementProject.TypeCode'].astype(str)
    df_outsiders_all['ContractFolderStatus.ProcurementProject.TypeCode'] = \
        df_outsiders_all['ContractFolderStatus.ProcurementProject.TypeCode'].apply(convert_to_object_array)

    df_outsiders_all['ContractFolderStatus.ProcurementProjectLot.ID'] = \
        df_outsiders_all['ContractFolderStatus.ProcurementProjectLot.ID'].apply(convert_to_object_array)
    df_outsiders_all['ContractFolderStatus.ProcurementProjectLot.ID'] = \
        df_outsiders_all['ContractFolderStatus.ProcurementProjectLot.ID'].apply(convert_to_ndarray)
        
    df_outsiders_all['link'] = df_outsiders_all.apply(extraer_url, axis=1)
    
    df_outsiders_all['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'] = \
        df_outsiders_all['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'].astype(str)
    df_outsiders_all['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'] = \
        df_outsiders_all['ContractFolderStatus.TenderResult.AwardedTenderedProject.LegalMonetaryTotal.TaxExclusiveAmount'].apply(convert_to_object_array)

    df_outsiders_all['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'] = df_outsiders_all['ContractFolderStatus.ProcurementProject.PlannedPeriod.DurationMeasure'].apply(
            lambda x: analyze_duration(x) if isinstance(x, str) else x
        )
    df_outsiders_all['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'] = \
        df_outsiders_all['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'].astype(str)
    df_outsiders_all['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'] = \
        df_outsiders_all['ContractFolderStatus.TenderResult.ReceivedTenderQuantity'].apply(convert_to_object_array)

    df_outsiders_all['ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.IDschemeName'] = \
            df_outsiders_all['ContractFolderStatus.TenderResult.WinningParty.PartyIdentification.IDschemeName'].apply(convert_to_object_array)

    df_outsiders_all['ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID'] = \
            df_outsiders_all['ContractFolderStatus.LocatedContractingParty.Party.PartyIdentification.ID'].apply(convert_to_object_array)

    df_outsiders_all['ContractFolderStatus.TenderingProcess.UrgencyCode'] = df_outsiders_all['ContractFolderStatus.TenderingProcess.UrgencyCode'].apply(replace_procedure_code)
    
    df_outsiders_all['ContractFolderStatus.TenderingTerms.FundingProgramCode']= \
        df_outsiders_all['ContractFolderStatus.TenderingTerms.FundingProgramCode'].apply(convert_to_object_array)
        
    df_outsiders_all['id'] = 'outsiders_gencat_opendata'
    
    df_outsiders_all['id'] = df_outsiders_all['id'].apply(convert_to_object_array)

    #import pdb; pdb.set_trace()
    df_outsiders_all.drop(columns=cols_drop, inplace=True, errors='ignore')

    # Guardar solo las columnas que se usarán
    output_path_final = os.path.join(input_dir, 'processed_gencat_outsiders_completo.parquet')
    df_outsiders_all.to_parquet(output_path_final, index=False)
    
      
def main():
    parser = argparse.ArgumentParser(description='Procesa y añade datos abiertos de Zaragoza, Madrid y Gencat.')
    parser.add_argument('--input_dir', type=str, required=True, help='Directorio de entrada que contiene opendata excel, json y csv')
    parser.add_argument('--administration', type=str, default='all', choices=['zaragoza', 'madrid', 'gencat', 'all'],
                        help='Administración a procesar (zaragoza, madrid, gencat o all)')
    parser.add_argument('--place_dir', type=str, required=True, help='Directorio donde se encuentran datos PLACE minors.parquet y outsiders.parquet')
    args = parser.parse_args()
    
    # Inicializar df
    df_minors = pd.DataFrame()
    df_minors_base = pd.DataFrame()

    input_dir = args.input_dir
    administration = args.administration
    place_dir = args.place_dir
    
    minors_path = os.path.join(place_dir, 'minors.parquet')
    outsiders_path = os.path.join(place_dir, 'outsiders.parquet')
    
    # Aplanar multiíndices de los dataframes
    if os.path.exists(minors_path):
        df_minors = pd.read_parquet(minors_path)
        df_minors.columns = [unify_colname(col) for col in df_minors.columns]
        print(f"Datos de minors cargados desde {minors_path}")
    else:
        print(f"No se encontró el archivo {minors_path}")

    if os.path.exists(outsiders_path):
        df_out = pd.read_parquet(outsiders_path)
        df_out.columns = [unify_colname(col) for col in df_out.columns]
        print(f"Datos de outsiders cargados desde {outsiders_path}")
    else:
        print(f"No se encontró el archivo {outsiders_path}")
        
    if administration in ['zaragoza', 'all']:
        # Integrar datos de Zaragoza con minors
        logging.info("Comenzando el procesamiento e integración de datos de Zaragoza...")
        zaragoza_file = os.path.join(input_dir, 'contratos_menores_zaragoza.json')
        if os.path.exists(zaragoza_file):
            df_zaragoza = pd.read_json(zaragoza_file, orient='records')
            df_minors_base = process_zaragoza(df_zaragoza, df_minors)
            # Guardar los datos procesados
            output_path = os.path.join(input_dir, 'processed_zaragoza.parquet')
            #df_minors_base.to_parquet(output_path)
            logging.info(f"Datos de Zaragoza procesados y guardados en {output_path}")
        else:
            df_minors_base = df_minors.copy().reset_index(drop=True)
            logging.info("No se procesarán datos de Zaragoza.")
    
    if administration in ['madrid', 'all']:
        logging.info("Comenzando el procesamiento e integración de datos de Madrid...")
        if administration == 'all':
            df_minors_base = process_madrid(df_minors_base, input_dir)
            # Guardar los datos procesados
            output_path = os.path.join(input_dir, 'processed_madrid.parquet')
            df_minors_base.to_parquet(output_path)
            logging.info("Datos de Madrid guardados.")
        else:
            print("No debe entrar si es all")
            df_minors_base = df_minors.copy()
            df_minors_solo_madrid = process_madrid(df_minors_base, input_dir)
            # Guardar los datos procesados
            output_path = os.path.join(input_dir, 'processed_solo_madrid.parquet')
            df_minors_solo_madrid.to_parquet(output_path)
            logging.info("Datos de Madrid guardados.")

    if administration in ['gencat', 'all']:
        logging.info("Comenzando el procesamiento e integración de datos de Gencat...")
        if administration == 'all':
            process_gencat(df_minors_base, df_out, input_dir)
        else:
            logging.info("Solo se procesarán e integrarán los datos de Gencat...")
            df_minors_base = df_minors.copy()
            process_gencat(df_minors_base, df_out, input_dir)
        

if __name__ == '__main__':
    main()
