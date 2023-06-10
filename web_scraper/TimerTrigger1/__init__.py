import logging
import azure.functions as func
from bs4 import BeautifulSoup
import requests
import pandas as pd
from random import randint
from time import sleep
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import traceback
import datetime
import pytz

requests.adapters.DEFAULT_RETRIES = 5

def is_11pm_in_timezone(time_zone):
    tz = pytz.timezone(time_zone)
    current_time = datetime.datetime.now(tz)
    if current_time.hour == 23:
        return True
    else:
        False


def get_airport_txt(connect_str,container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client("airportURL.txt")
    blob_content = blob_client.download_blob().content_as_text()
    airports = blob_content.split("\n")

    return airports

def upload_to_storage(container_name,connect_str, blob_name,local_path):
    try:
        # Create a BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        # Create a ContainerClient object
        container_client = blob_service_client.get_container_client(container_name)

        # Upload the CSV file to Azure Blob Storage
        with open('/tmp/'+local_path, 'rb') as csv:
            upload_response = container_client.upload_blob(blob_name, csv)
            logging.info(f"Uploaded csv: {blob_name}")

    except Exception:
        print(traceback.format_exc())
    return


def scrape_page(url,airport_code,query_strings):
    print("Scraping: "+ url)
    data = []
    for query_string in query_strings:
        sleep(randint(1,2))
        response = requests.get(url+query_string)
        soup = BeautifulSoup(response.content, 'html.parser')
        flight_rows = soup.find_all(class_="flight-row")
        for i, row in enumerate(flight_rows):
        #Skip the header row
            if i == 0:
                continue

            try:
                origin = row.find('div', class_=['flight-col flight-col__dest', 'flight-col__dest-term','flight-col flight-col__dest-ter','flight-col flight-col__dest-term']).text.strip()
                arrive_depart = row.find('div', class_=['flight-col flight-col__hour', 'flight-col__hour']).text.strip()
                flight = row.find('div', class_=['flight-col flight-col__flight', 'flight-col__flight']).text.strip()
                airline = row.find('div', class_=['flight-col flight-col__airline', 'flight-col__airline']).text.strip()
                try:
                    terminal = row.find('div', class_=['flight-col flight-col__terminal', 'flight-col__terminal']).text.strip()
                except:
                    terminal = "no column"
                status = row.find('div', class_=['flight-col__status', 'flight-col flight-col__status flight-col__status--G', 'flight-col flight-col__status flight-col__status--Y']).text.strip()
                data.append((origin, arrive_depart, flight, airline, terminal, str(status), airport_code))
            except:
                print(f"One or more column couldn't be read in airport: {airport_code}, row {i}")
                continue

    data = pd.DataFrame(data, columns=['origin', 'arrive_depart', 'flight', 'airline', 'terminal', 'status','airport_code'])
    data['Ingestion Date'] = pd.Timestamp.now()
    return data


def main(mytimer: func.TimerRequest) -> None:

    connect_str = ""
    logging.info('Function triggered')

    airports = get_airport_txt(connect_str,"aviation499")

    endpoints = ["arrivals","departures"]
    query_strings = ["?tp=0","?tp=6","?tp=12","?tp=18"]

    for endpoint in endpoints:
        for airport in airports:
            airport_url,airport_code,airport_name, time_zone = airport.split(sep=",")
            if is_11pm_in_timezone(time_zone.strip()):

                url = airport_url + endpoint

                df = scrape_page(url, airport_code,query_strings)

                #Only landed (finalised data)
                df_landed_and_cancelled = df[df["status"].str.contains("Landed") | df["status"].str.contains("Cancelled")]
                df_landed_and_cancelled.to_csv('/tmp/'+'flight_data_temp.csv', index=False)

                YYYYMMDD = datetime.datetime.now().strftime("%Y%m%d")
                upload_to_storage('aviation499',connect_str,f"{endpoint}/{YYYYMMDD}/{airport_code}.csv" ,'flight_data_temp.csv')
                print(f"Uploaded: {endpoint}/{YYYYMMDD}/{airport_code}.csv") #status codeEEEEEEEEEEEEE
            else:
                pass
