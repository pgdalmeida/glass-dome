import scrapy
from scrapy.crawler import CrawlerProcess
import os
import logging
import unidecode


class ARSpider(scrapy.Spider):
    '''Spider to crawl and download data from the Assembleia's 
    open data page https://www.parlamento.pt/Cidadania/Paginas/DadosAbertos.aspx'''
    
    name = 'arspider'
    excluded_urls = []

    # set custom settings
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 3,
        'DOWNLOAD_DELAY': 0.5,
        'LOG_LEVEL':'INFO'
        }

    def clean_string(string):
        ''' Clean, normalize characters, replace white space by underscores, etc. 
        Shouldn't be used on directories as it removes forward slashes.'''

        string = string.strip()
        string = string.replace(' ', '_')
        string = string.replace('/', '')
        string = string.replace('  ', ' ')
        string = string.lower()
        string = unidecode.unidecode(string)
        return 

    # start_requests method
    def start_requests(self):
        logging.getLogger('scrapy').setLevel(logging.DEBUG)
        start_urls = ['https://www.parlamento.pt/Cidadania/Paginas/DadosAbertos.aspx']
        for url in start_urls:
            yield scrapy.Request(url = url, callback = self.parse_root)
    
    def parse_root(self, response):
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
            breadcrumbs = response.css('div.ms-rteStyle-details_page_title *::text').get() # first element is in title instead of breadcrums links
            breadcrumbs.extend(response.css('div.breadcrumb-container ::text').extract()) # gather remaining directory from breadcrumbs
            
            # clean, normalize characters, replace white space by underscores and remove unwanted elements of the list before creating a string
            breadcrumbs = [self.clean_string(bc) for bc in breadcrumbs if bc.replace('>', '').strip() != ''] 
            if 'inicio' in breadcrumbs:
                breadcrumbs.remove('inicio')
            if breadcrumbs[-1].startswith('numero'):
                del breadcrumbs[-1]
            # join is only applied if there is more than one element in breadcrumbs
            if len(breadcrumbs) > 1:
                directory = os.path.join(*breadcrumbs)
            else:
                directory = breadcrumbs[0]
            logging.info('Page: ' + directory)

            directory = os.path.join('data', directory)
            os.makedirs(directory, exist_ok=True)

            for url in urls_to_download: 
                file_name = url[url.find('fich=') + len('fich='):url.find('&Inline=true')] # The file name is located in-between these two tags
                file_name = file_name.replace('_json.txt', '.json')
                file_name = file_name.replace('.json.txt', '.json')
                file_name = file_name.replace('%c3%ba', 'u')
                file_name = filename.lower()
                file_path = os.path.join(directory, file_name)
                logging.info('Downloading: ' + file_name)
                yield response.follow(url=url, callback=self.download_file, meta={'file_path':file_path})
        
        for url in urls_to_follow:
            self.excluded_urls.append(url)
            logging.debug('##################### following link:')
            logging.debug(url)
            yield response.follow(url=url, callback=self.parse_generic)

    def download_file(self, response):
        with open(response.meta['file_path'], 'wb') as f:
            f.write(response.body)








if __name__ == '__main__':

    # Run the Spider
    process = CrawlerProcess()
    process.crawl(ARSpider)
    process.start()
