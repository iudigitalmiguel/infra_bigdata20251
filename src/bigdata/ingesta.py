import requests
import json



def onteenr_datos_api(url="", params={}):
    url = "{}/{}/{}/{}/".format(url, params["characters"], params["{id}"], params["anime"])

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as error:
        print(error)
        return {}
    
parametros = {"characters":"characters","{id}":"20","anime":"anime"}
url = "https://api.jikan.moe/v4"

datos =onteenr_datos_api(url=url, params= parametros)

if len(datos) > 0:
    print(json.dumps(datos, indent=4))
else:
    print("No se obtuvo la consulta")