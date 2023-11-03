# Import scrapy
import scrapy

# Import the CrawlerProcess
from scrapy.crawler import CrawlerProcess

# Import os to create the file structure if it doesn't
import os


# Create the Spider class
class DiarioSpider(scrapy.Spider):
    name = 'diariospider'

    # set custom settings
    custom_settings = {
        'DOWNLOAD_DELAY': 0.5
        }

    # start_requests method
    def start_requests( self ):
        start_urls = ['https://www.parlamento.pt/DAR/Paginas/DAR1Serie.aspx']
        for url in start_urls:
            yield scrapy.Request(url = url, callback = self.parseDiario)
      
    def parseDiario(self, response):
        pdf_links = response.css('div.row.margin_h0.margin-Top-15 a::attr(href)').extract()
        pdf_titles = response.css('div.row.margin_h0.margin-Top-15 a::text').extract()
        pdf_info = list(zip(pdf_links, pdf_titles))
        for pdf_link, pdf_title in pdf_info: 
            yield response.follow(url=pdf_link, callback=self.download_pdf, meta={'pdf_title':pdf_title})

    def download_pdf(self, response):
        pdf_title = response.meta['pdf_title'].strip().replace(' ', '_') + '.pdf'
        self.log(f'Downloading file: {pdf_title}')
        with open('DAR_I/' + pdf_title, 'wb') as f:
            f.write(response.body)
        self.log(f'Saved file {pdf_title}')

# Create file structure to store pdfs if it doesn't exist
os.makedirs("DAR_I", exist_ok=True)

# Run the Spider
process = CrawlerProcess()
process.crawl(DiarioSpider)
process.start()
