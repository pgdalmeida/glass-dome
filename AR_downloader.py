import requests 
from requests.adapters import HTTPAdapter, Retry
import os
import pandas as pd
import logging
import time
import datetime
import hashlib
from tqdm import tqdm

def get_last_run(runs_dir):
    ''' Retrieve data on the last crawler run on a given directory'''

    runs_dir_list = []
    for f in  os.listdir(runs_dir):
        run_csv_path = os.path.join(runs_dir, f)
        if os.path.isfile(run_csv_path) and run_csv_path.endswith('.csv'):
            runs_dir_list.append(run_csv_path)
    runs_dir_list.sort(reverse=True) # list sorted from recent to older dates
    return runs_dir_list[0] # most recent log selected


if __name__ == '__main__':
    
    # setup the requests to make the get() method resilient to failiures 
    # (see https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request)
    s = requests.Session()
    retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('http://', HTTPAdapter(max_retries=retries))

    run_name = get_last_run('AR_crawler_runs')
    logging.info(f'Crawler run to download: {run_name}')
    with open(run_name, 'r') as run:
        run_df = pd.read_csv(run, keep_default_na=False)

    # checksum to store the md5 checksum of each downloaded file
    # Not only to keep track of which files were downloaded but also to check if they were changed
    
    if len(run_df.columns)==4:
        run_df['download_time'] = ''
        run_df['download_status'] = 0
        run_df['download_md5'] = ''

    file_not_downloaded = pd.Series([not os.path.isfile(f) for f in run_df['file_path']])
    run_to_download_df = run_df[(file_not_downloaded) & (run_df['download_status'] != 200)]
    
    # Setup progress bar for the command line 
    pbar = tqdm(total=len(run_to_download_df), unit='files')

    # try-catch to save the csv file in case of a keyboard interrupt
    try:
        for index, row in run_to_download_df.iterrows():
            file_path = row.loc['file_path']
            download_url = row.loc['file_url']
            download_status = row.loc['download_status']
            download_md5 = row.loc['download_md5']
            download_time = row.loc['download_time']
            
            logging.info(f'Downloading file: {file_path}')
            logging.debug(f'url: {download_url}')
            pbar.update(1) # update progress bar
            pbar.set_description(f'Downloading: {file_path}')
            
            # check if the file has already been downloaded, and if yes, skip it
            if os.path.isfile(file_path) and download_time != '' and download_status != 0 and download_md5 != '':
                pbar.refresh()
                continue

            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            downloaded_file = s.get(download_url)

            # status code = 200 means the download went fine, anything else means something went wrong
            if downloaded_file.status_code == 200:    
                run_df.at[index, 'download_time'] = datetime.datetime.now()
                run_df.at[index, 'download_status'] = downloaded_file.status_code
                run_df.at[index, 'download_md5'] = hashlib.md5(downloaded_file.content).hexdigest()           
                with open(file_path, 'wb') as f:
                    f.write(downloaded_file.content)
            time.sleep(0.2) # waitting time to not overload the servers

    except KeyboardInterrupt:
        # results from AR_downloader to be saved in this folder
        run_df.to_csv(run_name, index=False)

    # results from AR_downloader to be saved in this folder
    run_df.to_csv(run_name, index=False)
    # close progress bar
    pbar.close()
