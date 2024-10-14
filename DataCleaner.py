import re
import pandas as pd
import json
from dateutil import parser


class DataCleaner:
    def __init__(self, drugs_file, pubmed_csv_file, pubmed_json_file, clinical_trials_file, tmpDir='/tmp/'):
        self.drugs_file = drugs_file
        self.pubmed_csv_file = pubmed_csv_file
        self.pubmed_json_file = pubmed_json_file
        self.clinical_trials_file = clinical_trials_file
        self.tmpDir = tmpDir
        self.drugs_df = None
        self.pubmed_df_combined = None
        self.clinical_trials_df = None

    def parse_json(self, json_input_filepath, pattern=r',\s*([\]\}])'):
        try:
            with open(json_input_filepath, 'r') as file:
                json_data = file.read()
                cleaned_json_data = re.sub(pattern, r'\1', json_data)

                try:

                    return json.loads(cleaned_json_data)

                except json.JSONDecodeError as e:
                    print(f"Error: {e.msg} at line {e.lineno}, column {e.colno}")
        except FileNotFoundError:
            print("introuvable ou inaccessible")

    def parse_json(self, json_input_filepath, pattern=r',\s*([\]\}])'):
        try:
            with open(json_input_filepath, 'r') as file:
                json_data = file.read()
                cleaned_json_data = re.sub(pattern, r'\1', json_data)

                try:

                    return json.loads(cleaned_json_data)

                except json.JSONDecodeError as e:
                    print(f"Error: {e.msg} at line {e.lineno}, column {e.colno}")
        except FileNotFoundError:
            print("introuvable ou inaccessible")

    def json_to_df(self, json_parsed_Data):
        try:
            return pd.DataFrame(json_parsed_Data)
        except pd.errors.EmptyDataError:
            print("fichier vide")
        except pd.errors.ParserError:
            print("Erreur de parsing, revoir la fonction dataingestion.parse_json ")

    def standardize_date(self, date_column, target_format='%d/%m/%Y'):
        # Création d'une liste vide pour contenir les dates par la suite
        standardized_dates = []

        # parcours des dates par ligne
        for date in date_column:
            try:
                # Tentative de parsing simple à travers to_datetime
                parsed_date = pd.to_datetime(date, errors='raise', dayfirst=True)
            except (ValueError, TypeError):
                try:
                    # Si erreur , on essaye avec Dateutil.parser
                    parsed_date = parser.parse(date)
                except (ValueError, TypeError):
                    # si erreur ça retourne NaT
                    parsed_date = pd.NaT

            # If successfully parsed, format the date
            if pd.notna(parsed_date):
                standardized_dates.append(parsed_date.strftime(target_format))
            elif parsed_date.date():
                standardized_dates.append(pd.NaT)  # Append NaT for invalid dates

        return pd.Series(standardized_dates)

    def load_data(self):

        # Chargement des fichiers CSV sur les differents dataframes
        self.drugs_df = pd.read_csv(self.drugs_file)
        pubmed_csv_df = pd.read_csv(self.pubmed_csv_file)
        self.clinical_trials_df = pd.read_csv(self.clinical_trials_file)


        # Chargerment des données  JSON PubMed
        pubmed_json_df = self.json_to_df(self.parse_json(self.pubmed_json_file))

        # Combiner PubMed CSV and JSON data
        self.pubmed_df_combined = pd.concat([pubmed_csv_df, pubmed_json_df], ignore_index=True)

    def clean_pubmed_data(self):
        try:
            # Clean PubMed data (combined from CSV and JSON)
            self.pubmed_df_combined['date'] = self.standardize_date(self.pubmed_df_combined['date'], '%d/%m/%Y')
            self.pubmed_df_combined['date'] = pd.to_datetime(self.pubmed_df_combined['date'], errors='coerce',
                                                             dayfirst=True)
            self.pubmed_df_combined = self.pubmed_df_combined.sort_values(by='date').reset_index(drop=True)

            if self.pubmed_df_combined.empty:
                raise Exception("Pubmed est vide après le nettoyage ")
        except Exception as e:
            raise Exception(f"Erreur lors du nettoyage des donnees: {str(e)}")

    def clean_clinic_trials_data(self):
        try:
            # nettoyage de clinic_trials
            self.clinical_trials_df =self.clinical_trials_df.rename(columns={'scienscientific_title':'title'}, inplace=True)
            self.clinical_trials_df['date'] = self.standardize_date(self.clinical_trials_df['date'], '%d/%m/%Y')
            self.clinical_trials_df['date'] = pd.to_datetime(self.clinical_trials_df['date'], errors='coerce',dayfirst=True)

            if self.clinical_trials_df.empty:
                raise Exception(" clinic_trials est vide après le nettoyage ")
        except Exception as e:
            raise Exception(f"Erreur lors du nettoyage des donnees: {str(e)}")

    def save_cleaned_data(self):
        try:
            # Save cleaned data to temporary files
            self.drugs_df.to_csv(self.tmpDir + 'drugs_cleaned.csv', index=False)
            self.pubmed_df_combined.to_csv(self.tmpDir + 'pubmed_combined_cleaned.csv', index=False)
            self.clinical_trials_df.to_csv(self.tmpDir + 'clinical_trials_cleaned.csv', index=False)
        except IOError as e:
            raise Exception(f"Erreur lors de la sauvegarde des données nettoyé : {str(e)}")


    def execute(self):
        self.load_data()
        self.clean_pubmed_data()
        self.clean_clinic_trials_data()
        self.save_cleaned_data()
