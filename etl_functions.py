## Python script which include python functions for ETL_tool_data.py


##Imoprt libraries
import requests
import shutil
import os
import tarfile
import pandas as pd

#Current file path
current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
destination_path = f'{current_dir}/datasets'


## Function to download data set
def download_dataset(source_url):
    response = requests.get(source_url, stream = True)

    if response.status_code == 200:
        with open(f"{destination_path}/tolldata.tgz", 'wb') as file:
            file.write(response.raw.read())
    else:
        print("Failed to download the file")

#untar the file
def untar_dataset():
    with tarfile.open(f"{destination_path}/tolldata.tgz", 'r:gz') as tar:
        tar.extractall(path=f"{destination_path}")

# Function to extract data from the csv file
def extract_data_from_csv():
    in_csv_file = f"{destination_path}/vehicle-data.csv"
    out_csv_file = f"{destination_path}/csv_data.csv"

    df = pd.read_csv(in_csv_file)

    selected_data = df.iloc[:, [0, 1, 2 , 3]]

    selected_data.to_csv(out_csv_file, index=False)

# Function to extract tsv data 
def extract_data_from_tsv():
    in_tsv_file = f"{destination_path}/tollplaza-data.tsv"
    out_csv_file = f"{destination_path}/tsv_data.csv"

    df = pd.read_csv(in_tsv_file, sep = '\t')

    selected_data = df.iloc[:, [4, 5, 6]]

    selected_data.to_csv(out_csv_file, index = False)

# Function to extract data from fixed width
def extract_data_from_fixed_width():
    in_file = f"{destination_path}/payment-data.txt"
    out_csv_file = f"{destination_path}/fixed_width_data.csv"

    df = pd.read_fwf(in_file)

    selected_data = df.iloc[:, [9, 10]]

    selected_data.to_csv(out_csv_file, index = False)


# Function to cosolidate csv files
def consolidate_data():
    df_1 = pd.read_csv(f"{destination_path}/csv_data.csv")
    df_2 = pd.read_csv(f"{destination_path}/tsv_data.csv")
    df_3 = pd.read_csv(f"{destination_path}/fixed_width_data.csv")

    new_df = pd.concat([df_1, df_2, df_3], axis = 1)

    new_df.to_csv(f"{destination_path}/extracted_data.csv", index = False)

# Function to capitalize data
def transform_data():
    df = pd.read_csv(f"{destination_path}/extracted_data.csv", header=None)

    df = df.map(lambda x: x.upper() if isinstance(x, str) else x)

    df.to_csv(f"{destination_path}/transformed_data.csv", index = False, header = None)




    



