import pandas as pd

async def leerCsv(ruta):
    # Leer el archivo CSV y crear el DataFrame
    dtypes = {'SOURCE_ID': str}
    df = pd.read_csv(ruta ,sep=';', dtype=dtypes)   
    #df = pd.read_csv(ruta ,sep=';')   
    return df