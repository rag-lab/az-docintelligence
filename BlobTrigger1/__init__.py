import logging

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import azure.functions as func

import json
import time
import os
import requests
import numpy as np
import pandas as pd


def main(myblob: func.InputStream):

    try:

        config = get_keyvault_variables()

        apim_key = config['form-key']
        blob_conn_str = config['blob-connection-string']
        post_url = config['post_url']

        source = myblob.read()

        logging.warning(f"Python blob trigger function processed blob \n"
                f"Name: {myblob.name}\n"
                f"Local: {myblob.uri}\n"
                f"Blob Size: {len(source)} bytes")

        headers = {
            # Request headers
            'Content-Type': 'application/pdf',
            'Ocp-Apim-Subscription-Key': apim_key,
        }

        resp = requests.post(url=post_url, data=source, headers=headers)

        if resp.status_code != 202:
            print("POST analyze failed:\n%s" % resp.text)
            quit()
        print("POST analyze succeeded:\n%s" % resp.headers)


        get_url = resp.headers["operation-location"]
        wait_sec = 10
        time.sleep(wait_sec)
        # The layout API is async need the wait statement

        resp = requests.get(url=get_url, headers={"Ocp-Apim-Subscription-Key": apim_key})
        resp_json = json.loads(resp.text)

        status = resp_json["status"]

        if status == "succeeded":
            print("POST Layout Analysis succeeded:\n%s")
            results = resp_json
        else:
            print("GET Layout results failed:\n%s")
            quit()

        pages = results["analyzeResult"]["pageResults"]

        #extract the tables from page (p)
        def make_page(p):
            res=[] #store all the tables
            res_table=[] #track which table each cell belongs to (since a page may have multiple tables).
            y=0 #table counter.
            page = pages[p] #get specific page
            for tab in page["tables"]:
                for cell in tab["cells"]:
                    res.append(cell)
                    res_table.append(y)
                y=y+1

            #convert to pandas DF
            res_table=pd.DataFrame(res_table)
            res=pd.DataFrame(res)

            res["table_num"]=res_table[0]  #Adds a new column table_num to indicate which table each cell came from
            h=res.drop(columns=["boundingBox","elements"]) #drop unecessary columns
            h.loc[:,"rownum"]=range(0,len(h)) #numbers the rows sequentially
            num_table=max(h["table_num"]) #highest table index
            # h - cleaned DF of all cells on the page
            # total of tables on the page
            # page number passed
            return h, num_table, p 
    

        h, num_table, p= make_page(0)

        # Iterates over each table,
        # Extracts all cells that belong to that table,
        # Initializes a blank 2D grid of the right size (rows x columns),
        # Places each cell's text content into its corresponding (rowIndex, columnIndex) position.
        for k in range(num_table+1):
            new_table=h[h.table_num==k]
            new_table.loc[:,"rownum"]=range(0,len(new_table))
            row_table=pages[p]["tables"][k]["rows"]
            col_table=pages[p]["tables"][k]["columns"]
            b=np.zeros((row_table,col_table))
            b=pd.DataFrame(b)
            s=0
            for i,j in zip(new_table["rowIndex"],new_table["columnIndex"]):
                b.loc[i,j]=new_table.loc[new_table.loc[s,"rownum"],"text"]
                s=s+1

        # This is the connection to the blob storage, with the Azure Python SDK
        blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
        output_container_client=blob_service_client.get_container_client("output")

        #convert to CSV and save it to the output folder.
        fileName=os.path.basename(myblob.name)
        tab1_csv=b.to_csv(header=False,index=False,mode='w')
        name1=(os.path.splitext(fileName)[0]) +'.csv'
        output_container_client.upload_blob(name=name1,data=tab1_csv)
        return None

    except Exception as e:
        logging.error(f"Function failed with error: {e}", exc_info=True)


    
def get_secret(secret_name, vault_url):
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    return client.get_secret(secret_name).value


def get_keyvault_variables():

    try:

        """
        gets the key vault variables
        """
        key_vault_url = os.environ.get("KEY_VAULT_URL")

        config = {}
        config['form-endpoint'] = get_secret("formrecognizer-endpoint", key_vault_url)
        config['blob-connection-string'] = get_secret("blob-connection-string", key_vault_url)
        config['form-key'] = get_secret("formrecognizer-key", key_vault_url)

        if config['form-endpoint']:
            config['post_url'] = config['form-endpoint'] + "formrecognizer/v2.1/layout/analyze"
        
        # logging.warning(f"getting secrets from: '{key_vault_url}'")
        # for key, value in config.items():
        #     logging.warning(f"{key}: {value}")

        return config
    
    except Exception as e:
            logging.error(f"get_keyvault_variables() failed with error: {e}", exc_info=True)

