# Import scrapy
import scrapy

# Import the CrawlerProcess
from scrapy.crawler import CrawlerProcess

# Import os to create the file structure if it doesn't
import os

# Create the Spider class
class DARSpider(scrapy.Spider):
    '''Spider to crawl and download the "Diario da Assembleia da Républica. 
    Distributed by the www.parlamento.pt in json and xml formats. 
    It is also published in pdf but not downloaded'''
    
    name = 'darspider'

    # set custom settings
    custom_settings = {
        'DOWNLOAD_DELAY': 0.5
        }

    # start_requests method
    def start_requests( self ):
        start_urls = ['https://www.parlamento.pt/Cidadania/Paginas/DAdar.aspx']
        for url in start_urls:
            yield scrapy.Request(url = url, callback = self.parse_diario)
      
    def parse_diario(self, response):    
        links = response.css('div.archive-item a.TextoRegular::attr(href)').extract()
        for link in links:
            print(link)
            yield response.follow(url=link, callback=self.parse_diario_child)

    def parse_diario_child(self, response):
        file_links = response.css('div.archive-item a.TextoRegular[title$=".zip"]::attr(href)').extract()    
        folder_name = response.css('div.breadcrumb-container span+a::text').get()
        # Clean the folder_name string to create directories
        folder_name = folder_name.strip().replace(' ', '_').replace('é', 'e').replace('í', 'i').replace('á', 'a').replace('ç', 'c').lower()
        # Create directory to store zip files if it doesn't exist
        directory = os.path.join('DAR_zip_files', folder_name)
        os.makedirs(directory, exist_ok=True)

        for file_link in file_links: 
            file_link_substring = file_link[file_link.find('fich=') + len('fich='):]
            file_name = file_link_substring[:file_link_substring.find('&')]
            file_path = os.path.join(directory, file_name)
            yield response.follow(url=file_link, callback=self.download_file, meta={'file_path':file_path})

    def download_file(self, response):
        with open(response.meta['file_path'], 'wb') as f:
            f.write(response.body)

if __name__ == '__main__':

    # Run the Spider
    process = CrawlerProcess()
    process.crawl(DARSpider)
    process.start()
