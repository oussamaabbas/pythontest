from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.data_cleaner import DataCleaner
from scripts.mention_finder import MentionFinder

# Chemins des fichiers
drugs_file_path = '/tmp/drugs_cleaned.csv'
pubmed_file_path = '/tmp/pubmed_combined_cleaned.csv'
clinical_trials_file_path = '/tmp/clinical_trials_cleaned.csv'
output_mentions_file = '/tmp/mentions.json'

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'drug_mentions_pipeline',
    default_args=default_args,
    description='Pipeline pour extraire les mentions de médicaments',
    schedule_interval=timedelta(days=1),
)


# Tâche 1 : Nettoyer les données
def clean_data_task():
    cleaner = DataCleaner(
        drugs_file='/path/to/drugs.csv',
        pubmed_csv_file='/path/to/pubmed.csv',
        pubmed_json_file='/path/to/pubmed.json',
        clinical_trials_file='/path/to/clinical_trials.csv'
    )


try:
    cleaner.execute()
except Exception as e:
    raise Exception(f"Erreur dans la tâche de nettoyage des données : {str(e)}")

clean_data_op = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data_task,
    dag=dag
)


# Tâche 2 : Trouver les mentions dans les journaux
def find_mentions_task():
    mention_finder = MentionFinder(
        drugs_file=drugs_file_path,
        pubmed_file=pubmed_file_path,
        clinical_trials_file=clinical_trials_file_path
    )
    try:
        # Charger les données nettoyées
        mention_finder.load_cleaned_data()
        # Trouver les mentions et sauvegarder les résultats
        mention_finder.find_mentions(output_file=output_mentions_file)
    except Exception as e:
        raise Exception(f"Erreur dans la tâche de recherche de mentions : {str(e)}")


find_mentions_op = PythonOperator(
    task_id='find_mentions',
    python_callable=find_mentions_task,
    dag=dag
)

# Définition des dépendances entre les tâches
clean_data_op >> find_mentions_op
