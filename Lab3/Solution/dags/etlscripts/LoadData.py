import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine


class GCPDataLoader:

    def __init__(self):
        self.bucket_name = 'data_center_lab3'
    
    def getcredentials(self):
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
        return credentials
        
    def connect_to_gcp_and_get_data(self, file_name):
        gcp_file_path = f'transformed_data/{file_name}'

        credentials_info = self.getcredentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

        blob = bucket.blob(gcp_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def get_data(self, file_name):
        df = self.connect_to_gcp_and_get_data(file_name)
        return df


class PostgresDataLoader:

    def __init__(self):
        self.db_config = {
            'dbname': 'shelter_outcomes_db',
            'user': 'postgres',
            'password': 'pgadmin',
            'host': '34.27.8.179',
            'port': '5432',
        }

    def get_queries(self, table_name):
        
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"

        if table_name =="animaldimension":
            query = """CREATE TABLE IF NOT EXISTS animaldimension (
                            animal_key INT PRIMARY KEY,
                            animal_id VARCHAR,
                            name VARCHAR,
                            dob DATE,
                            reprod VARCHAR,
                            gender VARCHAR, 
                            animal_type VARCHAR NOT NULL,
                            breed VARCHAR,
                            color VARCHAR,
                            datetime TIMESTAMP
                        );
                        """
            alter_table_query = """ALTER TABLE animaldimension
                                ADD CONSTRAINT animal_key_unique UNIQUE (animal_key);
                                """
        elif table_name =="outcomedimension":
            query = """CREATE TABLE IF NOT EXISTS outcomedimension (
                            outcome_type_key INT PRIMARY KEY,
                            outcome_type VARCHAR NOT NULL
                        );
                        """
            alter_table_query = """ALTER TABLE outcomedimension
                                ADD CONSTRAINT outcometype_key_unique UNIQUE (outcome_type_key);
                                """
        elif table_name =="datedimension":
            query = """CREATE TABLE IF NOT EXISTS datedimension (
                            date_key INT PRIMARY KEY,
                            year_recorded INT2  NOT NULL,
                            month_recorded INT2  NOT NULL
                        );
                        """
            alter_table_query = """ALTER TABLE datedimension
                                ADD CONSTRAINT date_key_unique UNIQUE (date_key);
                                """
        else:
            query = """CREATE TABLE IF NOT EXISTS outcomesfact (
                            outcome_id SERIAL PRIMARY KEY,
                            animal_key INT,
                            date_key INT,
                            outcome_type_key INT,
                            FOREIGN KEY (animal_key) REFERENCES animaldimension(animal_key),
                            FOREIGN KEY (date_key) REFERENCES datedimension(date_key),
                            FOREIGN KEY (outcome_type_key) REFERENCES outcomedimension(outcome_type_key)
                        );
                        """
            alter_table_query = ";"
        return f"{drop_table_query}\n{query}\n{alter_table_query}".strip()
        #return f"{query}"

    def connect_to_postgres(self):
        connection = psycopg2.connect(**self.db_config)
        return connection

    def create_table(self, connection, table_query):
        print("Executing Create Table Queries...")
        cursor = connection.cursor()
        cursor.execute(table_query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def load_data_into_postgres(self, connection, gcp_data, table_name):
        cursor = connection.cursor()
        print(f"Dropping Table {table_name}")
        truncate_table = f"DROP TABLE {table_name};"
        cursor.execute(truncate_table)
        connection.commit()
        cursor.close()
        
        print(f"Loading data into PostgreSQL for table {table_name}")
        
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        gcp_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Number of rows inserted for table {table_name}: {len(gcp_data)}")
        
def load_data_to_postgres(file_name, table_name):
    gcp_loader = GCPDataLoader()
    table_data_df = gcp_loader.get_data(file_name)

    postgres_dataloader = PostgresDataLoader()
    table_query = postgres_dataloader.get_queries(table_name)
    postgres_connection = postgres_dataloader.connect_to_postgres()

    postgres_dataloader.create_table(postgres_connection, table_query)
    postgres_dataloader.load_data_into_postgres(postgres_connection, table_data_df, table_name)
    