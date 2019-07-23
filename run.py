import re, requests, pandas as pd, os, datetime, multiprocessing as mp, unidecode, googlemaps, dask.dataframe as dd, numpy as np
from dask.multiprocessing import get
from bs4 import BeautifulSoup

class GenericScraper():
    pass

class ListadoDirectoScraper():
    country = "Argentina"
    host = "listadodirecto.com"
    url_template = "https://listadodirecto.com/resultados?page={}"
    props = []
    def scrape(self):
        p, cont = 1, True
        while cont:
            soup = BeautifulSoup(requests.get(self.url_template.format(p)).content,"html.parser")
            props_html = soup.select("ul.row li div.card-body")
            for i,prop in enumerate(props_html):
                pvars = {}
                pvars["host"] = self.host
                pvars["country"] = self.country
                pvars["date"] = datetime.datetime.now().isoformat()
                pvars["operacion"] = "Venta" if prop.parent.findChildren("div",{"class":"compraventa"}) else "Alquiler"
                pvars["link"] = "https://" + self.host + prop.parent.parent.attrs["href"]
                cursor = props_html[i].next
                pvars["publish_date"] = re.sub("[^0-9/]","",cursor.text)
                cursor = cursor.next_sibling
                val_arr = cursor.text.split("+")
                pvars["valor"] = re.sub("[^0-9]","",val_arr[0])
                pvars["moneda"] = "USD" if "USD" in val_arr[0] else "ARS"
                pvars["expensas"] = int(re.sub("[^0-9]","",val_arr[1])) if len(val_arr)>1 else -1
                cursor = cursor.next_sibling
                val_arr = cursor.text.split("|")
                pvars["area"] = re.sub(r"(m2|[^0-9])","",val_arr[0])
                pvars["ambientes"] = int(re.sub("[^0-9]","",val_arr[1])) if len(val_arr)>1 else -1
                cursor = cursor.next_sibling
                pvars["tipo"] = cursor.text
                cursor = cursor.next_sibling
                barrio = re.search(r"\(([^\)]*)\)",cursor.text)
                pvars["barrio"] = unidecode.unidecode(barrio.group(1).upper())
                pvars["direccion"] = cursor.text.replace(barrio.group(0), "").strip()
                cursor = cursor.next_sibling
                pvars["titulo"] = cursor.text
                pvars["lat"] = None
                pvars["lng"] = None
                self.props.append(pvars)
            cont = not "disabled" in soup.select("ul.pagination li")[-2]["class"]
            p += 1
        return self

class SoloDuenosScraper():
    country = "Argentina"
    host = "www.soloduenos.com"
    zones = {1:"",2:" (GBA)"}
    zone_template = "https://www.soloduenos.com/BusquedaGeneral.asp"
    url_template = "https://www.soloduenos.com/ResultadoBusqueda.asp?whichpage=1&pagesize=10000&NroZona={}&sqlQuery=(a.CodInmueble=1)~(a.CodInmueble=2)~(a.CodInmueble=3)~(a.CodInmueble=4)~(a.CodInmueble=5)~(a.CodInmueble=6)~(a.CodInmueble=7)~(a.CodInmueble=8)~(a.CodInmueble=9)~(a.CodInmueble=10)"
    regexp = r"([^a-zA-Z0-9 /ñÑáéíóúÁÉÍÓÚÜ]|\ben\b)"
    opdict = {"alquiler":"Alquiler","venta":"Venta","ventaus":"Venta"}
    props = []
    def scrape(self):
        session = requests.Session()
        session.get(self.zone_template)
        for z,zb in self.zones.items():
            response = session.get(self.url_template.format(z))
            soup = BeautifulSoup(response.content, "html.parser")
            tit = soup.select_one("font.menubarrasuperiorB")
            props_trs_all = tit.find_next("table").findChildren("tr", recursive=False)
            props_trs = [t for i,t in enumerate(props_trs_all) if i % 6 in (1,3)]
            for i in range(0,len(props_trs),2):
                pvars = {}
                pvars["host"] = self.host
                pvars["country"] = self.country
                pvars["date"] = datetime.datetime.now().isoformat()
                pvars["link"] = "https://" + self.host + "/" + props_trs[i].select_one("a").attrs["href"]
                cursor = props_trs[i].select_one("div font")
                pvars["codigo"] = re.search(r"\d+", cursor.text).group(0)
                cursor = cursor.find_next("font").find_next("font")
                pvars["tipo"] = cursor.text
                cursor = cursor.next.next
                val_arr = re.sub(self.regexp,"",cursor).strip().split("/")
                pvars["barrio"] = unidecode.unidecode((val_arr[0] + zb).upper())
                pvars["ambientes"] = int(re.sub("[^0-9]","",val_arr[1])) if len(val_arr)>1 and val_arr[1]!="" else -1
                cursor = props_trs[i+1].select_one("div font")
                val_arr = re.sub(self.regexp,"",cursor.text).lower().split()
                pvars["operacion"] = self.opdict[val_arr[0]]
                pvars["moneda"] = "USD" if "us" in val_arr or val_arr[0][-2:].lower()=="us" else "ARS"
                pvars["valor"] = re.sub("[^0-9]","",val_arr[-1])
                cursor = cursor.find_next("font")
                pvars["area"] = re.sub(r"(m2|[^0-9])","",cursor.text)
                cursor = cursor.find_next("font")
                pvars["direccion"] = " ".join(re.sub(self.regexp,"",cursor.text).split()[1:])
                pvars["lat"] = None
                pvars["lng"] = None
                self.props.append(pvars)
        return self
    
class EnBAScraper():
    country = "Argentina"
    host = "www.enbuenosaires.com"
    ops = ["rental-standard","sale"]
    cities = ["capital-federal","zona-norte"]
    base_template = "https://www.enbuenosaires.com/"    
    url_template = "https://www.enbuenosaires.com/aviso-ajax-list.html?7-1.IBehaviorListener.0-&operationType=rental-standard&publisher_type=owner&summaryResults%3AorderBySelector=dateUpdated~desc&price_range=undefined~ARS~month$!$undefined~ARS~month&surface_area_range=undefined~undefined&current_page={}&_=1559614242475"
    props = []   
    def scrape(self):
        p, last_p = 0, None
        
        while not p==last_p:
            session = requests.Session()
            session.get(self.base_template)
            headers = {
                "Wicket-Ajax": "true",
                "Wicket-Ajax-BaseURL": "aviso-ajax-list.html?0"
            }
            response = session.get(self.url_template.format(p),headers=headers)
            soup = BeautifulSoup(response.content, "html.parser")
            props_html = soup.select("ul.snapproperty")
            if last_p is None: last_p = int(soup.select_one("ul.pagination li.active li.active").findChildren("li", recursive=False)[-1].text)
            for i,prop in enumerate(props_html):
                pvars = {}
                pvars["host"] = self.host
                pvars["country"] = self.country
                pvars["date"] = datetime.datetime.now().isoformat()
                cursor = props_html[i].find("a",{"itemprop":"name"})
                pvars["link"] = cursor.attrs["href"]
                found = re.search("^(.*) en ([^,]*)", cursor.text)
                pvars["direccion"] = found.groups()[0]
                pvars["barrio"] = unidecode.unidecode(found.groups()[1])
                cursor = props_html[i].select_one("li.descriptionproperty")
                metadata = cursor.text.split("-")
                pvars["moneda"] = "USD" if "us" in metadata[0].lower() else "ARS"
                pvars["valor"] = re.sub("[^0-9]","",metadata[0])
                pvars["tipo"] = metadata[1].strip()
                pvars["operacion"] = metadata[2].replace("24 Meses","").replace("En","").strip()
                pvars["ambientes"] = re.search("(\d+) Ambientes", metadata[3]).groups()[0] if re.search("(\d+) Ambientes", metadata[3]) else -1
                pvars["codigo"] = re.search(r"# (\d+)", metadata[3]).groups()[0]
                pvars["area"] = re.search(r"(\d+).*M",metadata[3]).groups()[0]
                pvars["lat"] = None
                pvars["lng"] = None
                self.props.append(pvars)       
            p += 1
        return self
    

    
class DeDuenosScraper():
    country = "Argentina"
    host = "www.deduenos.com"
    view_template = "https://www.deduenos.com/aviso/?aid={}"
    url_template = "https://www.deduenos.com/deduenos-api/avisos/getAvisoBusquedaHome.php"
    payload = {"filtros":[
    	{"filtro":"Tipo Operacion", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Tipo Inmueble", "valor":None, "descripcion":None, "show":True},
    	{"filtro":"Tipo Porpiedad", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Provincia", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Barrio", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Ambientes", "valor":None, "descripcion":"None Ambientes", "show":False},
    	{"filtro":"Antiguedad", "valor":None, "descripcion":"", "show":False},
    	{"filtro":"Precio", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Moneda", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Superficie Total", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Amenities", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Caracteristicas", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Estado Inmueble", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Disposicion", "valor":None, "descripcion":None, "show":False},
    	{"filtro":"Expensas", "valor":None, "descripcion":None, "show":False},    	
    	{"filtro":"Orden", "valor":"3", "descripcion":"Recientes", "show":False}
    ]}
    props = []   
    def scrape(self):
        props = requests.post(self.url_template, json=self.payload).json()["result"]
        for i,prop in enumerate(props):
            pvars = {}
            pvars["host"] = self.host
            pvars["country"] = self.country
            pvars["date"] = datetime.datetime.now().isoformat()
            pvars["codigo"] = prop["idavisos"]
            pvars["link"] = self.view_template.format(pvars["codigo"])
            pvars["direccion"] = prop["calle"]+" "+prop["altura"] 
            pvars["barrio"] = unidecode.unidecode(prop["barrioNombre"].upper())
            pvars["moneda"] = "USD" if prop["avisoMoneda"]=="dolares" else "ARS" if prop["avisoMoneda"]=="pesos" else "UNK"
            pvars["valor"] = prop["avisoPrecio"]
            pvars["tipo"] = prop["tpi_descripcion"].replace("Departamentos","Departamento")
            pvars["operacion"] = prop["tipoOperacion"]
            pvars["ambientes"] = prop["avisoDormitorios"]
            pvars["area"] = prop["avisoSuperficieTotal"]
            pvars["lat"] = None
            pvars["lng"] = None
            self.props.append(pvars)
        return self
    
    
class AlquilerDirectoScraper():
    country = "Argentina"
    host = "alquilerdirecto.com.ar"
    regexp = "[^0-9a-zA-zñÑáéíóúÁÉÍÓÚÜ /,]"
    url_template = "https://alquilerdirecto.com.ar/search?ciudad={}&barrio=&tipopropiedad=&cantidaddormitorios={}&sort=low&tipooperacion=&tipovendedor=&current-min=1000&current-max=50000&page={}"
    ambientes = {
        "Monoambiente": 1,
        "1-Dormitorio": 2,
        "2-Dormitorios": 3,
        "3-Dormitorios": 4,
        "4-Dormitorios": 5
    }
    ciudades = {
         "Buenos Aires": "-34.6036844,-58.3815591",
         #"La Plata": "-34.9204948,-57.95356570000001",
         #"San Miguel de Tucumán": "-26.8082848,-65.2175903",
         #"Mar del Plata": "-38.0054771,-57.5426106",
         #"Ciudad de Salta": "-24.7821269,-65.4231976",
         #"Ciudad de Santa Fe": "-31.6106578,-60.697294",
         #"Lanús": "-34.6994795,-58.39207949999999",
         #"Ciudad de Corrientes": "-27.4692131,-58.8306349",
         #"San Isidro": "-34.470829,-58.5286102",
         #"Vicente López": "-34.5209468,-58.4972596",
         #"Posadas": "-27.3621374,-55.90087459999999",
         #"Quilmes": "-34.7206336,-58.25460510000001",
         #"Pilar": "-34.4778621,-58.9091671",
         #"José C. Paz": "-34.5151052,-58.7662461",
         #"Gregorio de Laferrere": "-34.7497482,-58.58459089999999",
         #"Berazategui": "-34.762001,-58.2112961",
         #"González Catán": "-34.7627204,-58.6300804",
         #"Ciudad de San Luis": "-33.3017267,-66.3377522",
         #"Moreno": "-34.6340099,-58.791382",
         #"Isidro Casanova": "-34.7084659,-58.5858675",
         #"Ituzaingó": "-34.6570243,-58.6753978",
         #"Florencio Varela": "-34.7965806,-58.27601199999999",
         #"Lomas de Zamora": "-34.76118230000001,-58.4302476",
         #"Temperley": "-34.7678337,-58.3792534",
         #"Monte Grande": "-34.8271786,-58.4620124",
         #"San Justo": "-34.6874084,-58.5632629",
         #"Rafael Castillo": "-34.7084334,-58.6216473",
         #"Ramos Mejía": "-34.6549073,-58.5536355",
         #"Caseros": "-34.6094827,-58.5634631",
         #"Trujui": "-34.5956946,-58.7542213",
         #"Ezeiza": "-34.8150044,-58.53482839999999",
         #"Morón": "-34.6558611,-58.6167212",
         #"San Carlos de Bariloche": "-41.1334722,-71.3102778",
         #"Burzaco": "-34.8286232,-58.39333420000001",
         #"Monte Chingolo": "-34.728054,-58.3534546",
         #"Rafaela": "-31.2525979,-61.49164219999999",
         #"Remedios de Escalada (Partido de Lanús)": "-34.7212143,-58.39850999999999",
         #"La Tablada": "-34.6855199,-58.53200299999999",
         #"Olivos": "-34.5105515,-58.4963913",
         #"Villa Gobernador Gálvez": "-33.0255203,-60.6337608",
         #"Villa Luzuriaga": "-34.6717415,-58.59329990000001",
         #"Ciudad Evita": "-34.7175201,-58.53564119999999",
         #"Luján": "-34.5633312,-59.1208805",
         #"Necochea": "-38.5544968,-58.73960879999999",
         #"Don Torcuato": "-34.4938049,-58.62727150000001",
         #"Banda del Río Salí": "-26.8341103,-65.1654205",
         #"General Rodríguez": "-34.6020889,-58.9521294",
         #"Villa Tesei": "-34.6197923,-58.63921500000001",
         #"Villa Carlos Paz": "-31.4207828,-64.4992141",
         #"Sarandí": "-34.6832581,-58.3439941",
         #"Villa Domínico": "-34.695385,-58.3320999",
         #"Béccar": "-34.4640891,-58.53476089999999",
         #"Rafael Calzada": "-34.7871094,-58.35194779999999",
         #"Ushuaia": "-54.8019121,-68.3029511",
         #"Belén de Escobar": "-34.3402525,-58.7849434",
         #"Berisso": "-34.8730675,-57.88371729999999",
         #"Los Polvorines": "-34.4996258,-58.6914464",
         #"Lomas del Mirador": "-34.6663627,-58.5297986",
         #"Presidente Perón": "-34.9327739,-58.4059227",
         #"Villa Centenario": "-34.728169,-58.4283831",
         #"El Jaguel": "-34.8298225,-58.4938889",
         #"El Colmenar": "-26.7734361,-65.2036229"
    }
    props = []
    
    def scrape(self):
        for c in self.ciudades:
            for a, ambs in self.ambientes.items():
                p, cont = 1, True
                while cont:
                    url = self.url_template.format(c,a,p)
                    response = requests.get(url)
                    soup = BeautifulSoup(response.content, "html.parser")
                    props = soup.select("div.property")
                    for prop in props:
                        if prop.select_one("div.property-tag.featured").text=="Dueño Directo":
                            pvars = {}
                            pvars["host"] = self.host
                            pvars["country"] = self.country
                            pvars["date"] = datetime.datetime.now().isoformat()
                            pvars["operacion"] = prop.select_one("div.property-tag.sale").text.strip()
                            pvars["valor"] = re.sub("[^0-9]", "", prop.select_one("div.property-price").text.strip())
                            dir_barrio = re.sub(self.regexp, "", prop.select_one("h3.property-address").text).strip().split(",")
                            pvars["direccion"] = "" if len(dir_barrio)==1 else dir_barrio[0]
                            pvars["barrio"] = unidecode.unidecode(dir_barrio[-1].upper())
                            link = prop.select_one("h1.title a").attrs["href"]
                            split_link = link.split("-")
                            pvars["codigo"] = split_link[0]
                            pvars["tipo"] = split_link[2][0].upper()+split_link[2][1:]
                            pvars["link"] = "https://"+self.host+"/"+link
                            pvars["titulo"] = re.sub("[^0-9a-zA-ZñÑ /]", "", prop.select_one("h1.title").text)
                            pvars["moneda"] = "ARS"
                            pvars["ambientes"] = ambs
                            pvars["lat"] = None
                            pvars["lng"] = None
                            self.props.append(pvars)
                    p += 1
                    cont = soup.select_one("li.page-item.active") != soup.select("li.page-item")[-2]
        return self
    
class ZonaPropScraper():
    country = "Argentina"
    host = "www.zonaprop.com.ar"
    regexp = "[^0-9a-zA-zñÑáéíóúÁÉÍÓÚÜ /,]"
    url_template = "https://www.zonaprop.com.ar/{}-{}-{}{}-dueno-directo.html"
    zones = [
        "capital-federal-gba-norte-gba-sur-gba-oeste"
    ]
    ops = {
        "alquiler":"Alquiler",
        #"venta":"Venta"
    }
    tipos = {
        "departamentos":"Departamento",
        "ph":"PH",
        "casas":"Casa",
        "terrenos":"Terreno",
        "locales-comerciales":"Locales y Oficinas",
        #"oficinas-comerciales":"Locales y Oficinas",
        #"bodegas-galpones":"",
        #"bovedas-nichos-y-parcelas":"",
        #"cama-nautica":"",
        #"campos":"",
        #"casas-de-country":"",
        #"consultorios":"",
        #"departamento-en-country":"",
        #"depositos":"",
        #"desarrollos-horizontales":"",
        #"desarrollos-verticales":"",
        #"edificios":"",
        #"fondos-de-comercio":"",
        #"cocheras":"",
        #"hoteles":"",
        #"quintas-vacacionales":"",
        #"terrenos-de-country":""
    }
    props = []
    def scrape(self):
        for z in self.zones:
            for o, O in self.ops.items():
                for t, T in self.tipos.items():
                    p, cont = 1, True
                    while cont:
                        print(p)
                        rp = "" if p==1 else "-pagina-"+str(p)
                        url = self.url_template.format(t, o, z, rp)
                        response = requests.get(url)
                        soup = BeautifulSoup(response.content, "html.parser")
                        props = soup.select("div.posting-card")
                        for prop in props:
                            pvars = {}
                            pvars["codigo"] = prop.attrs["data-id"]
                            cursor = prop.findChild("div",{"class":"general-content"})
                            pvars["host"] = self.host
                            pvars["country"] = self.country
                            pvars["date"] = datetime.datetime.now().isoformat()
                            pvars["operacion"] = O
                            pvars["link"] = "https://"+self.host+cursor.select_one("a.go-to-posting").attrs["href"]
                            pvars["tipo"] = T
                            price = cursor.select_one("span.first-price")
                            pvars["valor"] = re.sub("[^0-9]","",price.text) if price else None
                            #pvars["expensas"] = int(re.sub("[^0-9]","",val_arr[1])) if len(val_arr)>1 else -1
                            pvars["titulo"] = re.sub(self.regexp,"",cursor.select_one("h3.posting-title").text)
                            pvars["direccion"] = cursor.select_one("span.posting-location").next.strip().replace(",","")
                            pvars["barrio"] = unidecode.unidecode(cursor.select_one("span.posting-location").next.next.text.strip().split(",")[0].upper())
                            pvars["moneda"] = "USD" if price and "USD" in price.text else "ARS"
                            match = re.search("(\d+) Ambientes",prop.select_one("ul.main-features").text)
                            pvars["ambientes"] = match.groups()[0] if match else -1
                            match = re.search("(\d+) \S+ totales",prop.select_one("ul.main-features").text)
                            pvars["area"] = match.groups()[0] if match else -1
                            pvars["lat"] = None
                            pvars["lng"] = None
                            self.props.append(pvars)
                        p += 1
                        a=soup.select_one("div.paging ul li.active")
                        cont = (not a is None) and (not a.next_sibling is None)and (not a.next_sibling.next_sibling is None)
        return self
    
if __name__ == "__main__":
    #p=AlquilerDirectoScraper().scrape().props
    #p=ZonaPropScraper().scrape().props
    #exit()
    latest = "Inmuebles.csv"
    history = "HistoryInmuebles.csv"
    #latest = "_.csv"
    #history = "__.csv"
    scrapers = [
        ListadoDirectoScraper(),
        SoloDuenosScraper(),
        DeDuenosScraper(),
        ZonaPropScraper(), #Bajar las coordenadas de la pagina
        AlquilerDirectoScraper(),
    ]
    def par_fn(scraper):
        return scraper.scrape().props
    p = mp.Pool(5)
    print("Started scraping")
    #pool_data = map(par_fn, scrapers)
    pool_data = p.map(par_fn, scrapers)
    print("Done scraping")
    print("Creating DataFrames")
    data_frames = [pd.DataFrame(d) for d in pool_data]
    latest_data = pd.concat(data_frames, sort=True)
    print("Done creating DataFrames")
    
    print("Retrieving history")
    history_df = None
    if os.path.isfile(history):
        history_df = pd.read_csv(history)
    print("Selecting new ones for geocoding")
    geo_fields = ["link","direccion","barrio","country"]
    no_lat = latest_data[np.logical_or(pd.isna(latest_data["lat"]),pd.isna(latest_data["lng"]))][geo_fields] #latest data with no lat, zonaprop would come with this filled
    
    to_geocode = no_lat
    if not history_df is None and history_df.shape[0]>0: 
        history_df = history_df[geo_fields+["lat","lng"]].drop_duplicates()
        to_geocode = no_lat[np.logical_and(~no_lat["link"].isin(history_df["link"]), ~no_lat["direccion"].isin(history_df["direccion"]))]
        #history_wc = history_df[~history_df["lat"].isnull()].drop_duplicates()
        #to_geocode = no_lat[~no_lat["link"].isin(history_wc["link"])]
        
        
    print("Geocoding {} addresses".format(to_geocode.shape[0]))
    gmaps = googlemaps.Client(key="hdp")
    
    def geocode_address(x):
        ret = gmaps.geocode(",".join([x["direccion"],x["barrio"],x["country"]]).replace("ZONA","Buenos Aires").replace("SUR","").replace("NORTE","").replace("OESTE",""))
        ret_val = [None, None]
        if ret and len(ret)>0: 
            ret_val = list(ret[0]["geometry"]["location"].values())
        return ret_val
    to_geocode.reset_index(inplace=True)
    d_geocode = dd.from_pandas(to_geocode, npartitions=30)
    
    to_geocode["coordenadas"] = d_geocode.map_partitions(lambda df: df.apply(geocode_address, axis=1)).compute(scheduler="processes") 

    to_geocode["lat"], to_geocode["lng"] = zip(*to_geocode.pop("coordenadas"))                         
    print("Done geocoding")
    print("Performing union")
    geo_codes = to_geocode
    if not history_df is None and history_df.shape[0]>0: 
        geo_codes = pd.concat([history_df, to_geocode], sort=True)
    #TODO: Adjust this line, to just join the null ones and concat with the ones already having latlng (zonaprop i.e.)
    keep_cols = [c for c in latest_data.columns.values if not c in ["lat","lng"]]
    latest_geocoded = pd.merge(latest_data[keep_cols], geo_codes[["link","lat","lng"]], on="link")
    print("Saving DataFrames")
    if os.path.isfile(history):
        f, h = open(history, "a"), False
    else:
        f, h = history, True
    latest_geocoded.to_csv(f, header=h, index=False)
    latest_geocoded["latest"] = latest_geocoded.apply(lambda x: x["link"] in to_geocode["link"].tolist(), axis=1)
    latest_geocoded.to_csv(latest, header=True, index=False)
    print("Done saving DataFrame with total {} records".format(latest_geocoded.shape[0]))
    
      
