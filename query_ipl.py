
from datetime import datetime
import pytz
from influxdb_client import InfluxDBClient
import pandas as pd
import plotly.graph_objs as go
import re

url = 'http://ciblink.leonisa.com:8086/'
token = 'zgJIdkKhauSQQSTL_cRgO_vyA1htnld8WsXLRHyoWtQOixB_qVMnWHoaZYDynv0XtexynnkKqswkrxR_6PBdzQ=='
org = 'Cib'
bucket = 'Chequeos_Piso'
datos = []

with InfluxDBClient(url=url, token=token, org=org) as client:
    query_api = client.query_api()
    
    tables = query_api.query('from(bucket: "Chequeos_Piso") |> range(start:2024-05-15T00:00:00Z, stop: now()) |> filter (fn: (r) => r._measurement == "IPL")')
    #tables = query_api.query('from(bucket: "Chequeos_Piso") |> range(start:-10h, stop: now()) |> filter (fn: (r) => r._measurement == "IPL")')
    data_table = []
    data_start = []
    data_stop = []
    data_time = []
    data_value = []
    data_num_rollo_actual = []
    data_presencia_tela = []
    data_field = []
    data_referencia = []
    data_aprobado = []
    data_escala_0_95_1 = []
    
    for table in tables:
        for record in table.records:
            #print(record)
            #print("\n")
            #datos.append(record)
            data_table.append(record["table"])
            data_start.append(record["_start"])
            data_stop.append(record["_stop"])
            data_time.append(record["_time"])
            data_value.append(record["_value"])
            try:
                data_aprobado.append(record["CALIFICACION"])
            except KeyError:
                data_aprobado.append("NOCALIFICACION")
            try:
                # Intenta acceder al campo 'NUM_ROLLO_ACTUAL_t' en el registro
                data_num_rollo_actual.append(record["NUMERO_ROLLO"])
            except KeyError:
                # Si se produce un KeyError, maneja la excepción aquí
                # Registra el valor NOROLLO en lugar de NUM_ROLLO_ACTUAL_t
                data_num_rollo_actual.append("NOROLLO")
            data_field.append(record["_field"])
            try:
                #intenta acceder a referencia actual
                data_referencia.append(record["REFERENCIA"])
            except KeyError:
                data_referencia.append("NOREFERENCIA")

            try:
                if record["_field"] == "ANCHO":
                    data_escala_0_95_1.append(record["CALIF_Ancho"])
                elif record["_field"] == "ELONGACION_LARGO":
                    data_escala_0_95_1.append(record["CALIF_EL"])
                elif record["_field"] == "ELONGACION_ANCHO":
                    data_escala_0_95_1.append(record["CALIF_EA"])
                elif record["_field"] == "PESO":
                    data_escala_0_95_1.append(record["CALIF_Peso"])
                elif record["_field"] == "REPITE":
                    data_escala_0_95_1.append(record["CALIF_Repite"])
                else:
                    data_escala_0_95_1.append("NA") #Cualquier campo que no sea ANCHO, ELONGACION_LARGO o PESO será NA
            except KeyError:
                data_escala_0_95_1.append("NOCALIF") #por si no encuentra el tag se inició el 22/05
            
            
             

            
# Definir la zona horaria deseada (en este caso, Colombia)
zona_horaria_colombia = pytz.timezone('America/Bogota')

data_start = [fecha_hora.astimezone(zona_horaria_colombia) for fecha_hora in data_start] #zona horaria colombia
data_start = [fecha_hora.replace(tzinfo=None) for fecha_hora in data_start] #elimino informacion zona horaria

data_stop = [fecha_hora.astimezone(zona_horaria_colombia) for fecha_hora in data_stop]
data_stop = [fecha_hora.replace(tzinfo=None) for fecha_hora in data_stop]

data_time = [fecha_hora.astimezone(zona_horaria_colombia) for fecha_hora in data_time]
data_time = [fecha_hora.replace(tzinfo=None) for fecha_hora in data_time]

df = pd.DataFrame({"table":data_table,"_start":data_start,"_stop":data_stop,"_time":data_time,
                   "_value":data_value,"NUM_ROLLO_ACTUAL":data_num_rollo_actual,"_field":data_field,"CALIF":data_escala_0_95_1,"Referencia":data_referencia,"CALIFICACION":data_aprobado})


#elimino características de repite para telas empiezan con T --------------------
condicion1 = df["Referencia"].str.startswith("T")
condicion2 = df["_field"] == "REPITE"


condicion3 = df["Referencia"].str.match(r"CM|CJ|E")
condicion4 = df["_field"].isin(["ANCHO","PESO"])


condicion5 = df["Referencia"].str.match(r'^TL\d{3}$')
condicion6 = df["_field"].isin(["ELONGACION_LARGO","ELONGACION_ANCHO"])



df =  df.loc[~((condicion1 & condicion2) | (condicion3 & condicion4)) | (condicion5 & condicion6)]  #Perfecciono las entrega de datos para eliminar no confiables



#---------------------------------------------------------------------------------------
for indice, valor in df["NUM_ROLLO_ACTUAL"].items():
    if len(valor) > 15:
        match = re.search(r'(\w{3})0*(\d{6})(?:0{4,}|$)', valor)
        try:
            numero = match.group(2)
            #print(numero)
            df.at[indice,'NUM_ROLLO_ACTUAL'] = numero
        except AttributeError:
            df.at[indice,'errorAtribute'] ="ATRIBUTEERROR"
#print(df["NUM_ROLLO_ACTUAL"])




#leo las normas portafolio
df_normas =pd.read_excel("normas portafolio.xlsx")

for indice,valor in df.iterrows():
    referencia=df.at[indice,"Referencia"]
    el_min=df_normas.loc[df_normas["Referencia"] == referencia,"EL_MIN"].values
    el_max=df_normas.loc[df_normas["Referencia"] == referencia,"EL_MAX"].values
    ea_min=df_normas.loc[df_normas["Referencia"] == referencia,"EA_MIN"].values
    ea_max=df_normas.loc[df_normas["Referencia"] == referencia,"EA_MAX"].values
    peso_min=df_normas.loc[df_normas["Referencia"] == referencia,"PESO_MIN"].values
    peso_max=df_normas.loc[df_normas["Referencia"] == referencia,"PESO_MAX"].values   
    ancho_min=df_normas.loc[df_normas["Referencia"] == referencia,"ANCHO_MIN"].values
    ancho_max=df_normas.loc[df_normas["Referencia"] == referencia,"ANCHO_MAX"].values
    repite_min=df_normas.loc[df_normas["Referencia"] == referencia,"REPITE_MIN"].values
    repite_max=df_normas.loc[df_normas["Referencia"] == referencia,"REPITE_MAX"].values


    if valor["_field"] == "ELONGACION_LARGO":
        df.at[indice,'norma_min']= el_min
        df.at[indice,'norma_max'] = el_max
    elif valor["_field"] == "ELONGACION_ANCHO":
        df.at[indice,'norma_min']= ea_min
        df.at[indice,'norma_max'] = ea_max
    elif valor["_field"] =="ANCHO":
        df.at[indice,'norma_min'] = ancho_min
        df.at[indice,'norma_max'] = ancho_max
    elif valor["_field"] == "PESO":
        df.at[indice,'norma_min']= peso_min
        df.at[indice,'norma_max'] = peso_max
    elif valor["_field"] =="REPITE":
        df.at[indice,'norma_min'] = repite_min
        df.at[indice,'norma_max'] = repite_max
    else:
        df.at[indice,'norma_min'] = str('no norma')
        df.at[indice,'norma_max'] = str('no norma')
df = df.drop_duplicates(subset=["NUM_ROLLO_ACTUAL","_field"],keep='last')  #para modificar porprint(df)
df.to_excel("pinitos.xlsx")