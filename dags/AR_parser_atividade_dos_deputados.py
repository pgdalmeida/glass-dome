import json
import pandas as pd

def keys_endswith(dictionary, ending_characters):
    ''' 
    Function to be used when retrieving elements from JSON objects based on the last characters of the key string.
    Returns a list with all the matching elements.
    '''
    matching_keys = []
    for key in dictionary.keys():
        if key.endswith(ending_characters):
            matching_keys.append(key)
        if matching_keys == []:
            return ['']
        else:
            return matching_keys

file_path = '/home/pedro/Projects/glass-dome/backup/data_backup/atividade_dos_deputados/xv_legislatura/atividadedeputadoxv.json'
with open(file_path, 'r') as file:
    json = json.load(file)

mp_names = []
mp_parties = []
mp_statuses = []
act_types = []
act_ids = []
act_numbers = []
act_descriptions = []

for i in json['ArrayOfAtividadeDeputado']['AtividadeDeputado']:
    #####################################################
    #       Info on the member of parlamient (mp)       #
    #####################################################
    mp = i['deputado']
    mp_name = mp['depNomeParlamentar']
    mp_party = mp['depGP']['pt_ar_wsgode_objectos_DadosSituacaoGP']['gpSigla']
    mp_status_data = mp['depSituacao']['pt_ar_wsgode_objectos_DadosSituacaoDeputado']
    # when mps serve in different time frames, this element comes as a list
    if type(mp_status_data) == dict:
        mp_status = mp_status_data['sioDes']
        # ignore this mp if he is no longer an mp
        if 'sioDtFim' in mp_status_data.keys():
            continue
    elif type(mp_status_data) == list:
        mp_status = mp_status_data[-1]['sioDes']
        # ignore this mp if he is no longer an mp
        if 'sioDtFim' in mp_status_data[-1].keys():
            continue

    ####################################
    #       Info on activities         #
    ####################################
    mp_act_type_list = i['AtividadeDeputadoList']['pt_gov_ar_wsar_objectos_ActividadeOut']
    for act_type, act in mp_act_type_list.items():
        
        # in case data is missing for an activity type
        if act == None or act_type == 'dadosLegisDeputado':
            continue

        # keep going through to deeper nested levels until the activity or list of activities is reached -> len(act) > 1
        while len(act) == 1:
            act = act[next(iter(act))]

        if type(act) == dict:
            mp_names.append(mp_name)
            mp_parties.append(mp_party)
            mp_statuses.append(mp_status)
            act_types.append(act_type)
            id = [key for key in activity.keys() if key.endswith('Id')]
            if id == []:
                act_ids.append(-1)
            else:
                act_ids.append(activity[id[0]])
        elif type(act) == list:
            for activity in act:
                mp_names.append(mp_name)
                mp_parties.append(mp_party)
                mp_statuses.append(mp_status)
                act_types.append(act_type)
                id = [key for key in activity.keys() if key.endswith('Id')]
                if id == []:
                    act_ids.append(-1)
                else:
                    act_ids.append(activity[id[0]])


       

mps_df = pd.DataFrame()
mps_df['mp_name'] = pd.Series(mp_names)
mps_df['mp_party'] = pd.Series(mp_parties)
mps_df['mp_status'] = pd.Series(mp_statuses)
mps_df['act_type'] = pd.Series(act_types)
mps_df['act_id'] = pd.Series(act_ids)

print(mps_df.query('mp_status != "Efetivo"'))