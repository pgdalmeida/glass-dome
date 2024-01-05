import json
import pandas as pd
import os
import logging

file_path = '/home/pedro/Projects/glass-dome/backup/data_backup/atividade_dos_deputados/xv_legislatura/atividadedeputadoxv.json'
save_result_path = '/home/pedro/Projects/glass-dome/csv_data/atividade_dos_deputados/xv_legislatura/atividadedeputadoxv.csv'

def get_fist_existing_key(act_dict, keys_list):
    ''' 
    Test the existance of a list of keys in a dictionary and return the first element for which there is a key.
    If no key is present, returns an empty sring and issues a warning log entry. 
    '''
    for key in keys_list:
        if key in act_dict:
            return act_dict[key]
    logging.warning(f'None of the keys in {keys_list} were found for: {act_dict}')
    return ''

def dequote(s):
    """
    If a string has single or double quotes around it, remove them.
    Make sure the pair of quotes match.
    If a matching pair of quotes is not found,
    or there are less than 2 characters, return the string unchanged.
    """
    s.replace('\n', '')
    s = s.strip()
    if (len(s) >= 2 and s[0] == s[-1]) and s.startswith(("'", '"')):
        return s[1:-1].strip()
    return s.strip()



with open(file_path, 'r') as file:
    json = json.load(file)

mp_id_column = []            # member of parliament (deputado) identifier from the original data source
mp_name_column = []          # member of parliament (deputado) name, the simplified name or parliamentary name 
mp_party_column = []         # member of parliament (deputado) party
mp_status_column = []        # member of parliament (deputado) status, as in currently active or not
act_id_column = []           # activity identifier created here by hashing the whole activity entry
act_old_id_column = []       # activity identifier from the original data source
act_type_column = []         # activity type, based on what data structure it was sourced from
act_subtype_column = []      # activity subtype, further information on the type of activity
act_dar_column = []          # best guess at where this activity can be found in the "Diario da Republica" (often dificult to find)
act_date_column = []         # date of activity or inicial date when it spans multiple days
act_desc_column = []         # activity description for long form description when aplicable

#####################################################
#       Info on the member of parlamient (mp)       #
#####################################################
for i in json['ArrayOfAtividadeDeputado']['AtividadeDeputado']:
    mp = i['deputado']
    mp_id = mp['depId']
    mp_name = mp['depNomeParlamentar']
    mp_party = mp['depGP']['pt_ar_wsgode_objectos_DadosSituacaoGP']['gpSigla']
    mp_dist = mp['depCPDes'] 
    mp_status_data = mp['depSituacao']['pt_ar_wsgode_objectos_DadosSituacaoDeputado']
   
    # when mps serve in different time frames, this element comes as a list
    if type(mp_status_data) == dict:
        mp_status = mp_status_data['sioDes']
        # # ignore this mp if he is no longer an mp
        # if 'sioDtFim' in mp_status_data.keys():
        #     continue
    elif type(mp_status_data) == list:
        mp_status = mp_status_data[-1]['sioDes']
        # # ignore this mp if he is no longer an mp
        # if 'sioDtFim' in mp_status_data[-1].keys():
        #     continue

    ####################################
    #       Info on activities         #
    ####################################
    mp_act_types = i['AtividadeDeputadoList']['pt_gov_ar_wsar_objectos_ActividadeOut']
    activity_types_to_skip = [
            'dadosLegisDeputado',   # Just redundant MP data
            'cms',                  # comission memberships
            'dlE',                  # sporadic delegations
            'dlP',                  # permanent delegations
            'parlamentoJovens',     # youth parliament
            'scgt',                 # sub comissions and working group memberships
            'gpa'                   # grupo parlamentar amizade
        ]

    # first pass on the list of to make a uniform structure to be read later
    # since each activity type seems to be completely different from the next
    mp_act_types_cleaned = {}
    for act_type, act in mp_act_types.items():
        
        # in case data is missing for an activity type
        if act is None or act_type in activity_types_to_skip:
            continue    
        
        # the 'rel' (relatores) activity type contains 4 more datatypes nested inside 
        if act_type == 'rel':
            for key in act.keys():
                if key not in ['relatoresIniciativas', 'relatoresPeticoes', 'relatoresIniEuropeias', 'relatoresContasPublicas']:
                    logging.warning(f'activity type not recognized: {mp_id}, {mp_name}, {act_type}, {key}')
            if 'relatoresIniciativas' in act:
                rin_act = act['relatoresIniciativas']['pt_gov_ar_wsar_objectos_RelatoresIniciativasOut']
                if type(rin_act) == dict:
                    mp_act_types_cleaned['rin'] = [rin_act]
                else:
                    mp_act_types_cleaned['rin'] = rin_act
            if 'relatoresPeticoes' in act:
                rpe_act = act['relatoresPeticoes']['pt_gov_ar_wsar_objectos_RelatoresPeticoesOut']
                if type(rpe_act) == dict:
                    mp_act_types_cleaned['rpe'] = [rpe_act]
                else:
                    mp_act_types_cleaned['rpe'] = rpe_act
            if 'relatoresIniEuropeias' in act:
                rie_act = act['relatoresIniEuropeias']['pt_gov_ar_wsar_objectos_RelatoresIniEuropeiasOut']
                if type(rie_act) == dict:
                    mp_act_types_cleaned['rie'] = [rie_act]
                else:
                    mp_act_types_cleaned['rie'] = rie_act
            if 'relatoresContasPublicas' in act:
                rcp_act = act['relatoresContasPublicas']['pt_gov_ar_wsar_objectos_RelatoresContasPublicasOut']
                if type(rcp_act) == dict:
                    mp_act_types_cleaned['rcp'] = [rcp_act]
                else:
                    mp_act_types_cleaned['rcp'] = rcp_act
            continue

        # standedize the activity type field and translate to english
        if act_type == 'actP':
            act_type = 'par' # parliament
        elif act_type == 'audicoes':
            act_type = 'adt' # audition
        elif act_type == 'audiencias':
            act_type = 'adc' # audience
        elif act_type == 'deslocacoes':
            act_type = 'tra' # travel
        elif act_type == 'eventos':
            act_type = 'eve' # event
        elif act_type == 'intev':
            act_type = 'int' # intervention
        elif act_type not in ['ini', 'req', 'rin', 'rpe', 'rie', 'rcp']:
            logging.warning(f'Activity type not recognized: {act_type}')
        # other activity types that don't need to be altered
        # ini -> initiative
        # req -> requirement
        # rin -> relatores iniciativas
        # rpe -> relatores peticoes
        # rie -> relatores iniciativas europeias
        # rcp -> relatores contas publicas

        # keep going through to deeper nested levels until the activity or list of activities is reached -> len(act) > 1
        while len(act) == 1:
            act = act[next(iter(act))]
        
        # when there's only one activity per type, the JSON object is not nested in a list. 
        # Having the object inside a list avoids code duplication.
        if type(act) == dict:
            act = [act]

        mp_act_types_cleaned[act_type] = act

    # second pass on the list of activities, now to read the activities themselves
    for act_type, activities in mp_act_types_cleaned.items():
        # checking that activities is the expected datatype
        if type(activities) != list:
            logging.warning(f'List of activities is not list type: {activities}')

        for activity in activities:
            act_id = ''
            act_subtype = ''
            act_dar = ''
            act_date = ''
            act_desc = ''
            # each activity type has a different structure and therefore has to be read differently
            if act_type == 'par':
                act_old_id = activity['actId']
                act_subtype = activity['actTpdesc']
                act_dar = f"{activity['actNr']}/{activity['actSelLg']}"
                act_date = activity['actDtent']
                act_desc = dequote(activity['actAs'])
            elif act_type == 'adt':
                act_old_id = activity['actId']
                act_subtype = activity['actTpdesc']
                act_dar = f"{activity['actNr']}"
                act_date = activity['accDtaud']
                act_desc = dequote(get_fist_existing_key(activity, ['actAs', 'nomeEntidadeExterna']))
            elif act_type == 'adc':
                act_old_id = activity['actId']
                act_subtype = activity['actTpdesc']
                act_dar = f"{activity['actNr']}/{activity['actLg']}"
                act_date = activity['accDtaud']
                act_desc = dequote(get_fist_existing_key(activity, ['actAs', 'nomeEntidadeExterna']))
            elif act_type == 'tra':
                act_old_id = activity['actId']
                act_subtype = activity['actTp']
                act_dar = f"{activity['cmsAb']}/{activity['actLg']}"
                act_date = activity['actDtdes1']
                act_desc = dequote(activity['actAs'])
            elif act_type == 'eve':
                act_old_id = activity['actId']
                act_subtype = activity['tevTp']
                act_dar = f"{activity['cmsAb']}/{activity['actLg']}"
                act_date = activity['actDtent']
                act_desc = dequote(activity['actAs'])
            elif act_type == 'ini':
                act_old_id = activity['iniId']
                act_subtype = activity['iniTpdesc']
                act_dar = f"{activity['iniNr']}/{activity['iniTp']}/{activity['iniSelLg']}/{activity['iniSelNr']}"
                act_date = ''
                act_desc = dequote(activity['iniTi'])
            elif act_type == 'int':
                act_old_id = activity['intId']
                act_subtype = activity['tinDs']
                act_dar = f"{activity['pubTp']}/{activity['pubNr']}/{activity['pubLg']}/{activity['pubSl']}"
                act_date = activity['pubDtreu']
                act_desc = dequote(get_fist_existing_key(activity, ['intSu']))
            elif act_type == 'rin':
                act_old_id = activity['iniId']
                act_subtype = activity['iniTp']
                act_dar = f"{activity['iniNr']}/{activity['iniSelLg']}"
                act_date = ''
                act_desc = dequote(activity['iniTi'])
            elif act_type == 'rpe':
                act_old_id = activity['petId']
                act_subtype = 'relatores peticoes'
                act_dar = f"{activity['petNr']}/{activity['petSelLgPk']}/{activity['petSelNrPk']}"
                act_date = ''
                act_desc = dequote(activity['petAspet'])
            elif act_type == 'rie':
                act_old_id = activity['ineId']
                act_subtype = 'relatores iniciativas europeias'
                act_dar = f"{activity['leg']}"
                act_date = activity['ineDataRelatorio']
                act_desc = dequote(activity['ineTitulo'])
            elif act_type == 'rcp':
                act_old_id = activity['actId']
                act_subtype = 'relatores contas publicas'
                act_dar = ''
                act_date = ''
                act_desc = dequote(activity['actAs'])
            elif act_type == 'req':
                act_old_id = activity['reqId']
                act_subtype = activity['reqPerTp']
                act_dar = f"{activity['reqNr']}/{activity['reqLg']}/{activity['reqSl']}"
                act_date = activity['reqDt'].split()[0]
                act_desc = dequote(activity['reqAs'])
            else:
                logging.warning(f'Activity type not recognized: {mp_id}, {mp_name}, {act_old_id}, {act_type}')

            mp_id_column.append(mp_id)
            mp_name_column.append(mp_name)
            mp_party_column.append(mp_party)
            mp_status_column.append(mp_status)
            act_id_column.append(act_id)
            act_old_id_column.append(act_old_id)
            act_type_column.append(act_type)
            act_subtype_column.append(act_subtype)
            act_dar_column.append(act_dar)
            act_date_column.append(act_date)
            act_desc_column.append(act_desc)
                

mp_act_df = pd.DataFrame({
    'mp_id':mp_id_column,
    'mp_name':mp_name_column,
    'mp_party':mp_party_column,
    'mp_status':mp_status_column,
    'act_id':act_id_column,
    'act_type':act_type_column,
    'act_subtype':act_subtype_column,
    'act_dar':act_dar_column,
    'act_date':act_date_column,
    'act_desc':act_desc_column
})
mp_act_df.to_csv(save_result_path, index=False, sep='|')