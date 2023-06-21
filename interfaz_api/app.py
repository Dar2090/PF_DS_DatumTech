import requests
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
import pandas as pd

app = FastAPI()
templates = Jinja2Templates(directory="templates")

df1 = pd.read_parquet('interfaz_api/Datasets/archivo1.parquet')
df2 = pd.read_parquet('interfaz_api/Datasets/archivo2.parquet')
df = pd.concat([df1, df2])

us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

states = df[df['state'].isin(us_states)]['state'].unique().tolist()

category = ['burger', 'burgers', 'hamburger', 'hamburgers', 'hot dog', 'steakhouse', 'lunch', 'motel', 'patisserie', 'pizza', 'deli', 'diner', 'dinner', 'icecream', 'ice cream', 'hotel', 'hotels', 'seafood', 'cookie', 'crab house', 'cupcake', 'chocolate', 'churreria', 'cocktail', 'cocktails', 'coffee', 'coffees', 'tea', 'restaurant', 'restaurats', 'cheese', 'charcuterie', 'cafe', 'cafes', 'BBQ', 'bagel', 'bakery', 'bakerys', 'bar', 'bars', 'bar & grill', 'barbecue', 'beer', 'bistro', 'pastry shop', 'pastry shops', 'breakfast', 'brunch', 'buffet', 'burrito', 'cafeteria', 'cafeterias', 'cake', 'cakes', 'food', 'wine', 'wineries']

categories = df[df['categories'].isin(category)]['categories'].unique().tolist()

@app.get("/")
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "states": states, "categories": categories})

@app.post("/")
def index(request: Request, state: str = Form(...), categories: str = Form(...)):
    response = requests.get(f'https://api-recomendaciones.onrender.com/?state={state}&categoria={categories}')
    data = response.json()
    return templates.TemplateResponse("results.html", {"request": request, "results": data})

@app.get("/lugares_cercanos")
def sentimiento_index(request: Request):
    return templates.TemplateResponse("lugares_index.html", {"request": request, "states": states, "categories": categories})

@app.post("/lugares_cercanos")
def sentimiento_index(request: Request, state: str = Form(...), categories: str = Form(...)):
    response = requests.get(f'https://api-recomendaciones.onrender.com/lugares_cercanos?state={state}&categoria={categories}')
    data = response.json()
    return templates.TemplateResponse("lugares_results.html", {"request": request, "results": data})
