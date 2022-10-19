#written by William Wiskes 7/11/2022, published under MIT
import pandas as pd
import urllib3
from google.cloud import bigquery
import arcgis
from arcgis.geometry import Geometry
from arcgis import GIS
import geopandas as gpd
import json

def usgs(request):
    #using urllib we make a request to our API
    http = urllib3.PoolManager()
    r = http.request('GET', 'https://nas.er.usgs.gov/api/v2/occurrence/search?state=UT')
    response = r.data
    #read that request as JSON
    usgs = json.loads(response)
    df = pd.DataFrame(usgs['results'])
    #turn this data spatial (geojson)
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.decimalLongitude, df.decimalLatitude))
    #connect to AGOL to retrieve our regions layer
    gis = GIS("https://utahdnr.maps.arcgis.com/", "REMOVED", "REMOVED")
    region = gis.content.get('70b2a33851eb4b58a7174c7464e3226a')
    region_lyr = region.layers[0]
    fset_region = region_lyr.query(out_sr={'wkid': 4326})
    #convert the feature service to geojson
    gjson_string = fset_region.to_geojson
    gjson_dict = json.loads(gjson_string)
    region_gdf = gpd.GeoDataFrame.from_features(gjson_dict['features'])
    #enrich the API geojson through using a spatial join
    result = gpd.tools.sjoin(gdf, region_gdf, how="left")
    #clean up the results
    result['Centroid'] = result['Centroid Type']
    joined = pd.DataFrame(result.drop(columns=['geometry','references', 'Centroid Type']))
    #connect to bigquery
    PROJECT_ID = 'ut-dnr-dwr-analysis-dev'
    client = bigquery.Client(project=PROJECT_ID, location="US")
    dataset_id = 'AIS_prod'
    table_id = 'Inspections_USGS'
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    #write our data to bigquery
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(
        joined, table_ref, job_config=job_config
    )  
    job.result() 
    return "complete"