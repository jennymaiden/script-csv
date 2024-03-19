import asyncio
import json
import pandas as pd

async def getPayments():
    with open('payment.json', 'r') as archivo_json:
        datos = json.load(archivo_json)
    return datos

async def leerCsv(ruta):
    # Leer el archivo CSV y crear el DataFrame
    dtypes = {'SOURCE_ID': str, 'EXTERNAL_REFERENCE' : str}
    df = pd.read_csv(ruta ,sep=';', dtype=dtypes)   
    #df = pd.read_csv(ruta ,sep=';')   
    return df

def crearCsv(filas_filtradas, nombre_archivo):
    filas_filtradas.to_csv(nombre_archivo, index=False)

async def ejecutar_tareas():
    payments = await getPayments()
    dataFrameOriginal = await leerCsv('prod.csv')
    dataFrameStage = await leerCsv('febreroStage.csv')
    # dataFrameModificado = await leerCsv('modificado.csv')

    ultima_fila = dataFrameOriginal.tail(1)
    dataFrameOriginal = dataFrameOriginal.drop(dataFrameOriginal.index[-1])
    for payment in payments['id']:
        # print(payment)
        filtro = dataFrameOriginal['SOURCE_ID'] == str(payment)
        filtroStage = dataFrameStage['SOURCE_ID'] == str(payment)
        filaPayment = dataFrameOriginal.loc[filtro]
        arrayReferencesStage = dataFrameStage.loc[filtroStage]
        # print(filaPayment)
        dateRelease = ''
        for index, fila in filaPayment.iterrows():
            #print(fila)
            # Acceder a los valores de cada columna
            if fila['DESCRIPTION'] == 'payment':
                dataFrameOriginal.loc[coincidencia] = nueva_fila
                print(payment)
                print(dateRelease)
                credito = fila['NET_CREDIT_AMOUNT']
                grossAmount = fila['GROSS_AMOUNT']
                dateRelease = fila['DATE']
                dataFrameOriginal.at[index, 'DESCRIPTION'] = 'refund'
                dataFrameOriginal.at[index, 'NET_DEBIT_AMOUNT'] = credito
                dataFrameOriginal.at[index, 'NET_CREDIT_AMOUNT'] = 0.00
                dataFrameOriginal.at[index, 'GROSS_AMOUNT'] = grossAmount * -1
        
        for index, fila in arrayReferencesStage.iterrows():
            arrayReferencesStage.at[index, 'DATE'] = dateRelease
        

        dataFrameOriginal = pd.concat([dataFrameOriginal, arrayReferencesStage], ignore_index=True)
    
    dataFrameOriginal = pd.concat([dataFrameOriginal, ultima_fila], ignore_index=True)
    crearCsv(dataFrameOriginal, 'modificado.csv')
         
asyncio.run(ejecutar_tareas())
