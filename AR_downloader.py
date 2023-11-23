import requests
from requests.adapters import HTTPAdapter, Retry
import os
import pandas as pd
import logging
import time
import datetime
import hashlib

if __name__ == '__main__':
    runs_dir = 'AR_crawler_runs'

    runs_dir_list = []
    for f in  os.listdir(runs_dir):
        run_csv_path = os.path.join(runs_dir, f)
        if os.path.isfile(run_csv_path) and run_csv_path.endswith('.csv'):
            runs_dir_list.append(run_csv_path)

    runs_dir_list.sort(reverse=True) # list sorted from recent to older dates
    run_name = runs_dir_list[0] # most recent log selected

    print(f'Crawler run to download: {run_name}')

    with open(run_name, 'r') as run:
        run_df = pd.read_csv(run)

    # setup the requests to make the get() method resilient to failiures 
    # (see https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request)
    s = requests.Session()
    retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('http://', HTTPAdapter(max_retries=retries))

    # checksum to store the md5 checksum of each downloaded file
    # Not only to keep track of which files were downloaded but also to check if they were changed
    checksums = []
    download_status = []
    download_time = []
    for index, row in run_df.iterrows():
        file_path = row.loc['directory']
        download_url = row.loc['file_url']
        print(f'Downloading file: {file_path}')
        print(f'url: {download_url}')
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        downloaded_file = s.get(download_url)
        download_status.append(downloaded_file.status_code)
        download_time.append(datetime.datetime.now())
        if downloaded_file.status_code == 200:    # status code = 200 means the download went fine, anything else means something went wrong
            checksums.append(hashlib.md5(downloaded_file.content).hexdigest())
            with open(file_path, 'wb') as f:
                f.write(downloaded_file.content)
        else:
            checksums.append('')
        time.sleep(0.5) # waitting time to not overload the servers
    
    run_df['download_md5'] = pd.Series(checksums)
    run_df['download_status'] = pd.Series(download_status)
    run_df['download_time'] = pd.Series(download_time)
    output_csv = os.path.join('AR_downloader_runs', os.path.basename(run_name))
    # results from AR_downloader to be saved in this folder
    os.makedirs('AR_downloader_runs', exist_ok=True)
    run_df.to_csv(output_csv, index=False)
