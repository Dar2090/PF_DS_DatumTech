from fastapi import FastAPI
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors

df1 = pd.read_parquet('api_recomendaciones/Datasets/archivo1.parquet')
df2 = pd.read_parquet('api_recomendaciones/Datasets/archivo2.parquet')
df = pd.concat([df1, df2])

app = FastAPI()

@app.get("/")
def recomendar_sitios(state: str = None, categoria: str = None):
    if state is None or categoria is None:
        return 'Gracias por elegir el modelo de recomendación de Datum Tech. Con este servicio podrás descubrir los mejores lugares para visitar en tu estado, desde restaurantes y discotecas hasta hoteles y más. Solo tienes que ingresar al siguiente enlace: https://api-recomendaciones.onrender.com/docs y empezar a explorar las opciones que más te gusten.'
        
    df_1 = df[df['state'].str.contains(state, case=False)]
    df_2 = df_1[df_1['categories'].str.contains(categoria, case=False)]
    df_3 = df_2.query("avg_rating >= 4")

    devolver = ['name', 'address', 'avg_rating', 'categories', 'attributes']
    df_4 = df_3[devolver]

    features = ['latitude', 'longitude', 'avg_rating', 'review_count']
    X = df_3[features].values

    if df_4.empty:
        return "Not information found"
   
    similitud = cosine_similarity(X)

    Indice = 0
    Sitios_indice = similitud[Indice].argsort()[:-11:-1]
    Sitios_similares = df_4.iloc[Sitios_indice]

    # Para que este ordenado por sitio
    lista_sitios = Sitios_similares.to_dict(orient='records')

    return lista_sitios[:5]


@app.get("/lugares_cercanos")
def lugares_cercanos(state: str = None, categoria: str = None):
    if state is None or categoria is None:
        return 'Gracias por elegir el modelo de recomendación de Datum Tech. Con este servicio podrás descubrir los mejores lugares para visitar en tu estado, desde restaurantes y discotecas hasta hoteles y más. Solo tienes que ingresar al siguiente enlace: https://api-recomendaciones.onrender.com/docs y empezar a explorar las opciones que más te gusten.'
        
    df_1 = df[df['state'].str.contains(state, case=False)]
    df_2 = df_1[df_1['categories'].str.contains(categoria, case=False)]
    df_3 = df_2.query("avg_rating >= 4")

    devolver = ['name', 'address', 'avg_rating', 'categories', 'attributes']
    df_4 = df_3[devolver]

    features = ['latitude', 'longitude']
    X = df_3[features].values

    nbrs = NearestNeighbors(n_neighbors=6).fit(X)
    distances, indices = nbrs.kneighbors(X)

    Indice = 0
    Sitios_indice = indices[Indice][1:]
    Sitios_similares = df_4.iloc[Sitios_indice]

    # Para que este ordenado por sitio
    lista_sitios = Sitios_similares.to_dict(orient='records')

    df_5 = pd.DataFrame(lista_sitios)
    
    if df_5.empty:
        return "Not information found"

    
    return df_5.to_dict(orient='records')


