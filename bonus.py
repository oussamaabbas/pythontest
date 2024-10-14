def find_co_mentions(drug_name, mentions):
    journals_for_drug = set([mention['journal'] for mention in mentions if mention['drug'] == drug_name and mention['source'] == 'PubMed'])

    co_mentions = set()
    for mention in mentions:
        if mention['journal'] in journals_for_drug and mention['drug'] != drug_name and mention['source'] == 'PubMed':
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


def find_mentions(drugs_df, pubmed_df_combined, clinical_trials_df):
    mentions = []

    # Boucle sur les médicaments et recherche dans les titres des publications
    for drug in drugs_df['drug']:
        # Rechercher dans PubMed
        pubmed_mentions = pubmed_df_combined[pubmed_df_combined['title'].str.contains(drug, case=False, na=False)]

        # Rechercher dans Clinical Trials
        clinical_mentions = clinical_trials_df[clinical_trials_df['scientific_title'].str.contains(drug, case=False, na=False)]

        for _, row in pubmed_mentions.iterrows():
            mentions.append({
                'drug': drug,
                'journal': row['journal'],
                'date': row['date'],
                'source': 'PubMed'
            })

        for _, row in clinical_mentions.iterrows():
            mentions.append({
                'drug': drug,
                'journal': row['journal'],
                'date': row['date'],
                'source': 'Clinical Trials'
            })

    return mentions