import scrapy
from scrapy.crawler import CrawlerProcess
import os
import logging
import unidecode
import datetime
from tqdm import tqdm

def clean_string(string):
        ''' Clean, normalize characters, replace white space by underscores, etc. 
        Shouldn't be used on directories as it removes forward slashes.'''

        logging.debug(f'##################### string to be cleaned: {string}')
        string = unidecode.unidecode(string)
        string = string.strip()
        string = string.replace('/', '')
        string = string.replace('  ', '_')
        string = string.replace(' ', '_')
        string = string.lower()
        logging.debug(f'##################### cleaned string: {string}')
        return string

class ARSpider(scrapy.Spider):
    '''Spider to crawl and download data from the Assembleia's 
    open data page https://www.parlamento.pt/Cidadania/Paginas/DadosAbertos.aspx'''
    
    name = 'arspider'
    excluded_urls = []
    run_datetime = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    
    # Setup progress bar for the command line (estimated number of links to crawl = 7899)
    pbar = tqdm(total=7899, unit='files')
    # set custom settings
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 0.2,
        'AUTOTHROTTLE_MAX_DELAY': 0.5,
        'DOWNLOAD_DELAY': 0.5,
        'LOG_LEVEL':'WARNING',
        'LOG_FILE': os.path.join('..', 'AR_crawler_logs', f'{run_datetime}.log')
        }
    # list that will gather all crawled files urls and directories in csv format to be written to a file at the end of the crawler run
    crawler_output = ['crawl_time,file_path,parent_url,file_url']

    def start_requests(self):
        start_urls = ['https://www.parlamento.pt/Cidadania/Paginas/DadosAbertos.aspx']
        for url in start_urls:
            yield scrapy.Request(url = url, callback = self.parse_root)
    
    def parse_root(self, response):
        ''' Parse root page method for parsing the initial page only as its structure is a little different than the child pages. '''

        logging.debug('##################### In root page') 
        urls_to_follow = response.css('div.ConteudoTexto a[title=Recursos]::attr(href)').extract()
        for url in urls_to_follow:
            self.excluded_urls.append(url)
            logging.debug('##################### following link:')
            logging.debug(url)
            yield response.follow(url=url, callback=self.parse_generic)

    def parse_generic(self, response):    
        urls = []
        try:
            urls = response.css('div.archive-item a::attr(href)').extract()
        except:
            logging.error('##################### This is not an HTML page')
            logging.error(response.url)
        urls_to_follow = [url for url in urls if '.aspx' in url and url not in self.excluded_urls]
        urls_to_download = [url for url in urls if 'json.txt' in url]

        if urls_to_download != []:
            # build the local directory for the data based on the webpage hierarchy (a. k. a. breadcrumbs)
            breadcrumbs = response.css('div.ms-rteStyle-details_page_title *::text').getall() # first element is in title instead of breadcrums links
            breadcrumbs.extend(response.css('div.breadcrumb-container ::text').extract()) # gather remaining directory from breadcrumbs
            
            # clean, normalize characters, replace white space by underscores and remove unwanted elements of the list before creating a string
            breadcrumbs = [clean_string(bc) for bc in breadcrumbs if bc.replace('>', '').strip() != '']
            if 'inicio' in breadcrumbs:
                breadcrumbs.remove('inicio')
            if breadcrumbs[-1].startswith('numero'):
                del breadcrumbs[-1]
            
            # join is only applied if there is more than one element in breadcrumbs
            if len(breadcrumbs) > 1:
                directory = os.path.join(*breadcrumbs)
            else:
                directory = breadcrumbs[0]
            logging.info(f'Page: {directory}')

            directory = os.path.join('data', directory)
            # os.makedirs(directory, exist_ok=True)

            for url in urls_to_download: 
                file_name = url[url.find('fich=') + len('fich='):url.find('&Inline=true')] # The file name is located in-between these two tags
                file_name = file_name.replace('_json.txt', '.json')
                file_name = file_name.replace('.json.txt', '.json')
                file_name = file_name.replace('%c3%ba', 'u')
                file_name = file_name.lower()
                file_path = os.path.join(directory, file_name)
                self.crawler_output.append(f'{datetime.datetime.now()},{file_path},{response.url},{url}')
                self.pbar.update(1) # update progress bar
                self.pbar.set_description(f'Registering: {file_path}')
        
        for url in urls_to_follow:
            self.excluded_urls.append(url)
            logging.debug(f'##################### following link: {url}')
            yield response.follow(url=url, callback=self.parse_generic)
    
    def closed(self, reason):
        '''Runs automatically uppon crawler finishing its run. Writes out to a csv the crawled files and their respective file stricture.'''

        file_path = os.path.join('AR_crawler_runs', f'{self.run_datetime}.csv')
        with open(file_path, 'w') as f:
            f.write('\n'.join(self.crawler_output))
                
def start_ar_crawler():
    # Run the Spider
    os.makedirs('AR_crawler_runs', exist_ok=True) # make sure the 'crawler_runs' directory exists and if not create it
    os.makedirs('AR_crawler_logs', exist_ok=True)
    process = CrawlerProcess()
    process.crawl(ARSpider)
    process.start()

if __name__ == '__main__':
    start_ar_crawler()
    
