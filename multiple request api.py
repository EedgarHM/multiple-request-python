import copy
import aiohttp
import asyncio
import time
import json
import pandas as pd
import awswrangler as aws
from datetime import date
import sys
from awsglue.utils import getResolvedOptions
import numpy as np
import requests
import math
args = getResolvedOptions(sys.argv,['SATWS_KEY','is_kapitalFlex'])

redshift_con = aws.redshift.connect('Prod_DataLake')
users = pd.DataFrame()
users = aws.s3.read_csv('s3://kapital-banfin-master/rfc.csv')

rfcs = users['rfc']

str_rfc = '('

for rfc in rfcs:
    str_rfc +=','
    str_rfc += f"'{rfc}'"
str_rfc = str_rfc.replace(',','',1)
str_rfc +=')'

query = f"select least(max(createdat)::date,max(updatedat)::date)||'T00:00:00.000Z' minDate ,user_rfc from satws.invoices i where user_rfc in {str_rfc} group by user_rfc"

res_df = aws.redshift.read_sql_query(sql=query,con=redshift_con)
header={'X-API-Key':args['SATWS_KEY'], 'Accept':'application/ld+json'}

result = pd.merge(users,res_df,left_on='rfc',right_on='user_rfc',how='left')
result['mindate'] = result['mindate'].fillna('NA')
result = result.drop(columns=['user_rfc'])
print(result)

failed_url = []

BATCH_SIZE = 100


def get_lists(rfc,dates):
    """
        Funcion que obtiene la primer y ultima pagina según los registros que regrese la API
    """
    
    dte = '' if dates == 'NA' else f"&createdAt[after]={dates}&updatedAt[after]={dates}"
    request = []
    rfc_cliente=rfc
    url=f'https://api.satws.com/taxpayers/{rfc_cliente}/invoices?itemsPerPage=1000'
    r_facturas=requests.get(url,headers=header)
    if r_facturas is not None:
        r_facturas = r_facturas.json()
        print(url)
        print(r_facturas.keys())

    # Si se encuentran las facturas, obtenemos la primer y ultima pagina, si no, si el numero de facturas es menor a 1000 solo consultamos una vez
    if 'hydra:view' in r_facturas:
        first_page = None if not 'hydra:first' in r_facturas['hydra:view'] else r_facturas['hydra:view']['hydra:first']
        last_page = None if not 'hydra:last' in r_facturas['hydra:view'] else r_facturas['hydra:view']['hydra:last']
        invoices_num = r_facturas['hydra:totalItems']
        first_page = '&page=1' if invoices_num < 1000 else first_page
        last_page = '&page=1' if invoices_num < 1000 else last_page

        if not first_page is None:
            first_page_number = first_page.split('&page=')[1]
            last_page_number = last_page.split('&page=')[1]
            
            # Se crea una lista con N links segun la cantidad de páginas que haya 
            for n in range(int(first_page_number),int(last_page_number)+1):
                url = f'https://api.satws.com/taxpayers/{rfc_cliente}/invoices?itemsPerPage=1000&page={str(n)}'
                request.append(url)
    return request

def chunk_it(list_, number_):
    """
        Generador, regresa bloques de links según los parametros dados
    """
    for i in range(0, len(list_), number_):
        yield list_[i:i+number_]
    
# Obtenemos los links totales que vamos a solicitar a la API
links = []

for col, row in result.iterrows():
    # Itera sobre los RFC que se hayan encontrado
    print(f" Getting URLS")
    urls_rfcs = get_lists(row["rfc"],row["mindate"])
    
    print(f"::::::::::::::: [TOTAL NUMBER OF URLS] : [{len(urls_rfcs)}] :::::::::::::::")
    
    # Se crea una lista con todos los links
    for url in urls_rfcs:
        links.append(url)
        

# Genera una lista con la cantidad de elementos que le pasemos en BATCH_SIZE y toma como parametro la lista de links
chunk_urls = list(chunk_it(links, BATCH_SIZE))
print(f"::::::::::::::: LOTES TOTALES  : [{len(chunk_urls)}] :::::::::::::::")




async def fetch_data(session, url, max_retries = 4):
    retries = 0
    while retries < max_retries:
        try: 
            print(f'[FETCH DATA FOR URL]: [{url}]')
            async with session.get(url, headers = header) as response:
                
                if response.status == 200:
                    response_json = await response.json()
                    print(f"[STATUS CODE FOR] [{url}] IS [{response.status}] - Try Number: [{retries}]")
                    
                    if url in failed_url:
                        print(f"[URL SUCCESS] - [DELETING URL FROM FAILED URLS LIST] [{url}]::::: [LENGTH FAILED URLS] : [{len(failed_url)}]")
                        failed_url.remove(url)
                    
                    return response_json
                else: 
                    print(f"Status for [{url}] is [{response.status}] - [retrying...] Try Number [{retries}] ")
                    failed_url.append(url)
            retries += 1
        except Exception as e:
            print(f'Ocurrió un error del tipo : [{e}]\n [Petición a URL] : [{url}]')
            


async def fetch_data_per_block(session, lote):
    print(f"::::::::::::::: [FETCHING DATA PER BLOCKS] ::::::::::::::")
    tasks = [fetch_data(session, url) for url in lote]
    return await asyncio.gather(*tasks)
    
async def main(url_lotes):
    # creamos un dataframe el cual almacenará toda la información
    data = pd.DataFrame()
    """
        * Parametros
            * url_lotes : Lista que contiene listas y cada lista contiene 100 urls o bien el número que se le especifique
            
        Ejecuta consultas hacia la API creando una sesión y pasando elemntos por bloques
        guarda la información en un dataframe de pandas, este se almacena en s3 y espera 15 segundos para realizar otra petición de 100 links
    """
    async with aiohttp.ClientSession() as session:
        count = 1
        for lote in url_lotes:
            print(f"::::::::::::::: [MAIN] - [EXECUTING LOTE NUMBER] = [{count}] :::::::::::::::")
            results = await fetch_data_per_block(session, lote)
            print(f"::::::::::::::: [MAIN] - [RESULTS] = [{len(results)}] :::::::::::::")
            data = pd.concat([data,pd.DataFrame.from_dict(results)],ignore_index=True)
            count += 1
            await asyncio.sleep(15)
            print(f"- - - - - - - - - - - [DATA RESULTS] : [{len(data)}] - - - - - - - - - - -")
    
    print(f"- - - - - - - - - - - [FINAL DATA RESULTS] : [{len(data)}] - - - - - - - - - - -")
    if not data.empty:
        aws.s3.to_csv(data,path="s3://data-python-scripts/test/satws_invoices.csv",sep='|',index=False)
        print(data)
        print('Done!')
    else:
        print("The file is empty")
            
            
asyncio.run(main(url_lotes=chunk_urls))



