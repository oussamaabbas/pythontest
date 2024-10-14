import pandas as pd
import json
import os


class MentionFinder:
    def __init__(self, drugs_file, pubmed_file, clinical_trials_file):
        self.drugs_file = drugs_file
        self.pubmed_file = pubmed_file
        self.clinical_trials_file = clinical_trials_file

    def load_cleaned_data(self):
        try:
            # Charger les fichiers nettoyés
            if not os.path.exists(self.drugs_file):
                raise FileNotFoundError(f"Fichier des médicaments nettoyé non trouvé : {self.drugs_file}")
            if not os.path.exists(self.pubmed_file):
                raise FileNotFoundError(f"Fichier PubMed nettoyé non trouvé : {self.pubmed_file}")
            if not os.path.exists(self.clinical_trials_file):
                raise FileNotFoundError(f"Fichier Clinical Trials nettoyé non trouvé : {self.clinical_trials_file}")

            self.drugs_df = pd.read_csv(self.drugs_file)
            self.pubmed_df_combined = pd.read_csv(self.pubmed_file)
            self.clinical_trials_df = pd.read_csv(self.clinical_trials_file)

            if self.drugs_df.empty or self.pubmed_df_combined.empty or self.clinical_trials_df.empty:
                raise ValueError("Un des jeux de données nettoyés est vide.")
        except FileNotFoundError as e:
            raise e
        except pd.errors.EmptyDataError:
            raise Exception(f"Erreur : Un des fichiers nettoyés est vide.")
        except Exception as e:
            raise Exception(f"Erreur inattendue lors du chargement des données nettoyées : {str(e)}")

    def find_mentions(self, output_file):
        try:
            mentions = []
            for drug in self.drugs_df['drug']:
                # Rechercher les mentions dans PubMed
                pubmed_mentions = self.pubmed_df_combined[
                    self.pubmed_df_combined['title'].str.contains(drug, case=False, na=False)]

                # Rechercher les mentions dans Clinical Trials
                clinical_mentions = self.clinical_trials_df[
                    self.clinical_trials_df['title'].str.contains(drug, case=False, na=False)]

                # Ajouter les mentions provenant de PubMed
                for _, row in pubmed_mentions.iterrows():
                    mentions.append({
                        'drug': drug,
                        'journal': row['journal'],
                        'date': row['date'],
                        'source': 'PubMed'
                    })

                # Ajouter les mentions provenant de Clinical Trials
                for _, row in clinical_mentions.iterrows():
                    mentions.append({
                        'drug': drug,
                        'journal': row['journal'],
                        'date': row['date'],
                        'source': 'Clinical Trials'
                    })

            # Sauvegarder les résultats
            with open(output_file, 'w') as f:
                json.dump(mentions, f)
        except Exception as e:
            raise Exception(f"Erreur lors de la recherche de mentions : {str(e)}")

    def find_co_mentions(drug_name, mentions):
        journals_for_drug = set([mention['journal'] for mention in mentions if
                                 mention['drug'] == drug_name and mention['source'] == 'PubMed'])

        co_mentions = set()
        for mention in mentions:
            if mention['journal'] in journals_for_drug and mention['drug'] != drug_name and mention[
                'source'] == 'PubMed':
                co_mentions.add(mention['drug'])
        return co_mentions

    def journal_with_most_mentions(mentions):
        journal_count = {}

        for mention in mentions:
            journal = mention['journal']
            if journal not in journal_count:
                journal_count[journal] = set()
            journal_count[journal].add(mention['drug'])

        # Trouver le journal avec le plus de médicaments mentionnés
        max_journal = max(journal_count, key=lambda x: len(journal_count[x]))
        return max_journal, len(journal_count[max_journal])
