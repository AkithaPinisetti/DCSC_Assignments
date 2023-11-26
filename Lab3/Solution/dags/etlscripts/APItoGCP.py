import pytz
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage


mountain_time_zone = pytz.timezone('US/Mountain')


def extract_data_from_api(limit=50000, order='animal_id'):
    """
    Function to extract data from data.austintexas.gov API.
    """
    api_url = 'https://data.austintexas.gov/resource/9t4d-g238.json'
    
    api_key = '58778d3tul9ykaurce5wf29ek'
    
    headers = { 
        'accept': "application/json", 
        'apikey': api_key,
    }
    
    loop = 0
    data = []

    while loop < 157000:  
        params = {
            '$limit': str(limit),
            '$offset': str(loop),
            '$order': order,
        }

        api_response = requests.get(api_url, headers=headers, params=params)
        print("response : ", api_response)
        latest_data = api_response.json()
        
        if not latest_data:
            break

        data.extend(latest_data)
        loop += limit

    return data


def create_dataframe(data):
    columns = [
        'animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
        'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
        'breed', 'color'
    ]

    df_list = []
    for entry in data:
        row_df = [entry.get(column, None) for column in columns]
        df_list.append(row_df)

    df = pd.DataFrame(df_list, columns=columns)
    return df


def upload_to_gcs(dataframe, bucket_name, file_path):
    """
    Upload a DataFrame to a Google Cloud Storage bucket using service account credentials.
    """
    print("Writing data to GCS.....")
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

    client_info = storage.Client.from_service_account_info(credentials)
    csv_df = dataframe.to_csv(index=False)
    
    bucket = client_info.get_bucket(bucket_name)
    
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    file_path_formatted = file_path.format(current_date, current_date)
    
    blob = bucket.blob(file_path_formatted)
    blob.upload_from_string(csv_df, content_type='text/csv')
    print(f"Completed writing data to GCS with date: {current_date}.")


def main():
    data_extracted = extract_data_from_api(limit=50000, order='animal_id')
    shelter_data = create_dataframe(data_extracted)

    gcs_bucket_name = 'data_center_lab3'
    gcs_file_path = 'data/{}/outcomes_{}.csv'

    upload_to_gcs(shelter_data, gcs_bucket_name, gcs_file_path)
    