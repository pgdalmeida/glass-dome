import json
import pandas as pd
from pathlib import Path
import os
import logging
import re

file_path = '/home/pedro/Projects/glass-dome/backup/data_backup/iniciativas/iniciativasxv.json'
save_result_path = '/home/pedro/Projects/glass-dome/csv_data/iniciativas/iniciativasxv.csv'

with open(file_path, 'r') as file:
    json = json.load(file)

id_column = []                      # id composed of iniNr/iniTipo/iniLeg/iniSel
type_column = []                    # type of initiative e.g. "Apreciação Parlamentar"
title_column = []                   # title of the initiative
text_url_column = []                # url to the text of the initiative
entry_date_column = []              # date for when it originally put forward
publication_date_column = []        # date for when it was published
dar_ref_column = []                 # the DAR identified with pages where the text was published
dar_url_column = []                 # url link to the original DAR document in pdf
admission_date_column = []          # date for when the initiative was admitted
announcement_date_column = []       # date for when the initiative was announced
author_list_column = []             # list of authors (mps) as a single string
number_authors_column = []          # number of authors (mps)
party_column = []                   # party responsible for the initiative

json_data = json['ArrayOfPt_gov_ar_objectos_iniciativas_DetalhePesquisaIniciativasOut']['pt_gov_ar_objectos_iniciativas_DetalhePesquisaIniciativasOut']
for ini in json_data:
    ######## id
    id = f"{ini['iniNr']}/{ini['iniTipo']}/{ini['iniLeg']}/{ini['iniSel']}"
    id_column.append(id)

    ######## party
    party = None
    if 'iniAutorGruposParlamentares' in ini:
        parties = ini['iniAutorGruposParlamentares']['pt_gov_ar_objectos_AutoresGruposParlamentaresOut']
        if type(parties) == list:
            party = ''
            for p in parties:
                party += f"{p['GP']} "
            party = party.strip() # remove trailing space
        else:
            party = parties['GP']
    party_column.append(party)

    ######## title
    title = ini['iniTitulo'].strip().replace('\\n', ' ').replace('\n', ' ')
    title_column.append(title)

    ######## text_url
    text_url = ini['iniLinkTexto']
    text_url_column.append(text_url)

    ######## entry_date, publication_date
    entry_date = None
    publication_date = None
    dar_ref = None
    dar_url = None
    eventos = ini['iniEventos']['pt_gov_ar_objectos_iniciativas_EventosOut']
    if type(eventos) != list:
        eventos = [eventos]
    for evento in eventos:
        if evento['fase'] == 'Entrada':
            entry_date = evento['dataFase']
        elif evento['fase'] in ['Publicação', 'Votação Deliberação']:
            # if this event doesn't actually contain any publication data, skip it 
            # there's likely publication data in another event
            if evento.get('publicacaoFase') is None:
                continue
            dar_list = evento['publicacaoFase']['pt_gov_ar_objectos_PublicacoesOut']
            # lists with a single object are not stored as list in the JSON file
            # by making it a list, all data can be retrieved in the same way
            if type(dar_list) != list:
                dar_list = [dar_list]
            dar_ref = ''
            publication_date = ''
            for entry in dar_list:
                publication_date += f"{entry['pubdt']} "
                if 'pag' in entry:
                    pages = f"{entry['pag']['string']}/"
                else:
                    pages = ''

                dar_cat = re.sub(r'DAR ', '', entry['pubTipo'])   # change format from "DAR II série A" to "II-A"
                dar_cat = re.sub(r' série ', '-', dar_cat)
                dar_ref += f"{pages}{entry['pubNr']}/{dar_cat}/{entry['pubLeg']}/{entry['pubSL']} "

            dar_ref = dar_ref.strip()
            publication_date = publication_date.strip()
    entry_date_column.append(entry_date)
    publication_date_column.append(publication_date)
    dar_ref_column.append(dar_ref)

    ######## author_list
    mp_names = []
    authors_list = []
    if 'iniAutorDeputados' in ini:
        mps = ini['iniAutorDeputados']['pt_gov_ar_objectos_iniciativas_AutoresDeputadosOut']
        if type(mps) != list:
            mps = [mps]
        authors_list += mps
    
    mp_names = [author['nome'] for author in authors_list]
    author_list_column.append(', '.join(mp_names))
    number_authors_column.append(len(mp_names))


ini_df = pd.DataFrame({
    'id': id_column,
    'party': party_column,
    'title': title_column,
    'text_url': text_url_column,
    'entry_date': entry_date_column,
    'publication_date': publication_date_column,
    'dar_ref': dar_ref_column,
    'authors': author_list_column,
    'number_authors': number_authors_column,
    'party': party_column
})
directory = os.path.dirname(save_result_path)
Path(directory).mkdir(parents=True, exist_ok=True)
ini_df.to_csv(save_result_path, index=False, sep='|')

