import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

mountain_time_zone = pytz.timezone('US/Mountain')

outcomes_map = {'Rto-Adopt':1, 
                'Adoption':2, 
                'Euthanasia':3, 
                'Transfer':4,
                'Return to Owner':5, 
                'Died':6,
                'Disposal':7,
                'Missing': 8,
                'Relocate':9,
                'N/A':10,
                'Stolen':11}


def getcredentials():
    bucket = "data_center_lab3"
    credentials = {
                          "type": "service_account",
                          "project_id": "plasma-backbone-406301",
                          "private_key_id": "53530bf15a0dca8f4a9461773f4f05ccaac30715",
                          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCjk2xy50zNSYWF\nRyncGAaJMQPBMbNdcGgyeZXK+tVtEKMKpsMi+4bEVqqCrOHaPGJVhN+JAsyyTJXP\nGpifqiuA0HMBbkspDm8rgzbXJMyuNuau7KbcANrQDZ0OB9d6F6lkgHXcChfVL7bh\n8fZuwAUVjtVEfGijBjmNm88iCQkWOpX/Uqsb1xpcYd7pj5BLqQUk8zqqIpkjnx/B\ndFtGfhZEzB4fxMMwhLEagzlOGdnN9ftZtxumgE5cmJ/sJYJf0xvQATJY9YIDj1AT\nmJA5Tcc/JnvwwG0462WtD7hEkTuw8k6v7Xb6oOuLarHTKGko279NrBqHc3uLmLlb\nfFYf1j0PAgMBAAECggEABzRDL9ZUvvRnfRb76Of3+ktC+ySblKGShmBJMKzfk1Od\nLhTXa7hbxQt8YtkZXsdGL20HDd2mV43zgIx89izyGtTBHQ6pacJGxUoKV+s5NDq/\nxXPCCvt1OeeJ3wOJQdvQfo8A9CDh9IrByW2QciL0/4Jr1pAdjaWpUobKjWJC6x6Q\n90MHLUf+FX9QWgOrL8noct7C4r7219oV3O8el7iKJF0qCzhD+k4JSj0nNbrbNRuG\n5wyY1ZekyrVhfs3ry8TQsxf1C1Lwha+tWI1gYuHXKyPVR9eeOjtGrL0/1QPimJf9\nDNidZrmy45mGhkkRA6NjVg+wTwecdM/VBQ+O2yaECQKBgQDSn35HjTRRkIFDilRx\neIR/GOeDj8q3Irvx0mSGJlkZqGvmXwadDrVAfS20sMGfUMplamaRMI9S5umffbQg\nlHxOIW/lGJjb0KJt59y1ttGccLazNbHq+9lxMkIEIHjRVwhZ9ES/m1VwL8CkOCho\nJ7bEXHN7zoMKpdFN0l8BH9yMtwKBgQDG0SK5F9+aDlfJLZ/VJmEMXFuWBqxN7xd+\n4jJRsLCaFNNbIFux2wGx9P5EfljMQMF7hDr60W4jdfJw6NcpP44hugoYjJHad01+\nJiU59wOVnRdqL/G1f1yRH4At+B2yCt/exvPpljMefsgaTivJELgBB1Dz1FVgAIUM\nVuHNDYmqaQKBgB9KInBuwb78QLfP7QuOY+Cdyob47ZyXRGSAZP6o48O0CZOHumvK\nq5KRBiE5wQnx7p9yVxpqpGAkfcB75C6S4ISa4wydwtek/vxk3Z0BM9KRzBKDf5Lx\nJzRxyuziBhDTZSI3756nbOHltjCvRxFyFOzG70ENRNpoF9f/0K1SFmmxAoGAEDV2\nrj5rBWVL9OPaVwdU/Cv/b4DFxWjLspWAYraT/0vZW2GM+DgRsE9391+Rn71byNUj\n9dTjNNLl3ByvhfZfgRJoxk2XiocVc0Zq3Int6eGvygF0pEZo5o/55EWJLj3CuKfh\ntNaA/mh5qeNboH3TcooFKIvUFgqUzke7CvuikJkCgYEAlPXtYPH5853lZsBY1g5x\nGr/yYDk9XyzGZEvFtF8qfWTO1UoRnf8AiizpNlNRTXNermpaFEZCu1aNvgQaTnII\nTTYddOw+rFPyuTzdhEC0nPDVcSm8DhvMmN4gEN3KapPUILxBspzAK7r02NlVlZmH\nbHFjcpjpWRUY+S1y6k8iCDE=\n-----END PRIVATE KEY-----\n",
                          "client_email": "akithap@plasma-backbone-406301.iam.gserviceaccount.com",
                          "client_id": "111057168878740365259",
                          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                          "token_uri": "https://oauth2.googleapis.com/token",
                          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/akithap%40plasma-backbone-406301.iam.gserviceaccount.com",
                          "universe_domain": "googleapis.com"
                  }
    return credentials, bucket


def getdata(credentials_inf, gcp_bucket):
    gcp_filepath = 'data/{}/outcomes_{}.csv'

    client_info = storage.Client.from_service_account_info(credentials_inf)
    
    bucket_id = client_info.get_bucket(gcp_bucket)
    

    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    
    formatted_file_path = gcp_filepath.format(current_date, current_date)
    
    blob = bucket_id.blob(formatted_file_path)
    csv_file = blob.download_as_text()
    data = pd.read_csv(StringIO(csv_file))

    return data


def write_data_to_gcs(dataframe, credentials_info, bucket_name, file_path):
    print(f"Writing data to GCS.....")

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS.")


def prep_animal_tab_dim(data):
    print("Preparing Animal Dimensions Table Data")
    animal_dim_tab_data = data[['animal_id','name','date_of_birth', 'reprod', 'animal_type', 'breed', 'color','gender','datetime']].drop_duplicates()
    animal_dim_tab_data['animal_key'] = range(1, len(animal_dim_tab_data) + 1)
    return animal_dim_tab_data


def prep_date_tab_dim(data):
    date_dim_tab_data = data[['month_recorded', 'year_recorded']].drop_duplicates()
    date_dim_tab_data['date_key'] = range(1, len(date_dim_tab_data) + 1)
    return date_dim_tab_data


def prep_outcome_types_tab_dim(data):
    outcome_type_dim_tab_data = data[['outcome_type']].drop_duplicates()
    outcome_type_dim_tab_data['outcome_type_key'] = range(1, len(outcome_type_dim_tab_data) + 1)
    return outcome_type_dim_tab_data



def prep_outcomes_fct_tab(data, animal_dim_data, date_dim_data, outcome_type_dim_data):
    df_fact = data.merge(date_dim_data, how='inner', left_on=['month_recorded','year_recorded'], right_on=['month_recorded','year_recorded'])
    df_fact = df_fact.merge(animal_dim_data, how='inner', left_on=['animal_id','animal_type','datetime'], right_on=['animal_id','animal_type','datetime'])
    df_fact = df_fact.merge(outcome_type_dim_data, how='inner', left_on='outcome_type', right_on='outcome_type')

    df_fact.rename(columns={
        'date_key': 'date_key',
        'animal_key': 'animal_key',
        'outcome_type_key': 'outcome_type_key'
    }, inplace=True)

    df_fact = df_fact[['date_key', 'animal_key', 'outcome_type_key']]
    return df_fact

def transform(data):
    transformed_data = data.copy()
    transformed_data['monthyear'] = pd.to_datetime(transformed_data['monthyear'])
    transformed_data['month_recorded'] = transformed_data['monthyear'].dt.month
    transformed_data['year_recorded'] = transformed_data['monthyear'].dt.year
    transformed_data[['Name']] = transformed_data[['name']].fillna('Name_less')
    transformed_data.drop(['monthyear','age_upon_outcome'], axis=1, inplace = True)
    mapping = {
    'Animal ID': 'animal_id',
    'Name': 'name',
    'datetime': 'datetime',
    'date_of_birth': 'date_of_birth',
    'outcome_type': 'outcome_type',
    'animal_type': 'animal_type',
    'breed': 'breed',
    'color': 'color',
    'month_recorded': 'month_recorded',
    'year_recorded': 'year_recorded',
    'sex_upon_outcome': 'sex'
    }
    transformed_data.rename(columns=mapping, inplace=True)
    transformed_data[['reprod', 'gender']] = transformed_data.sex.str.split(' ', expand=True)
    transformed_data.drop(columns = ['sex'], inplace=True)
    return transformed_data

def transform_data():
    credentials, bucket = getcredentials()

    rawdata = getdata(credentials, bucket)
    
    rawdata = transform(rawdata)
    
    dim_animal_tab = prep_animal_tab_dim(rawdata)
    dim_date_tab = prep_date_tab_dim(rawdata)
    dim_outcome_tab = prep_outcome_types_tab_dim(rawdata)

    outcomes_fact_tab = prep_outcomes_fct_tab(rawdata,dim_animal_tab,dim_date_tab,dim_outcome_tab)

    dim_animal_path = "transformed_data/dim_animal.csv"
    dim_dates_path = "transformed_data/dim_dates.csv"
    dim_outcome_types_path = "transformed_data/dim_outcome_types.csv"
    fct_outcomes_path = "transformed_data/fct_outcomes.csv"

    write_data_to_gcs(dim_animal_tab, credentials, bucket, dim_animal_path)
    write_data_to_gcs(dim_date_tab, credentials, bucket, dim_dates_path)
    write_data_to_gcs(dim_outcome_tab, credentials, bucket, dim_outcome_types_path)
    write_data_to_gcs(outcomes_fact_tab, credentials, bucket, fct_outcomes_path)
    