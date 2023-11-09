# glass-dome
Tools to improve the scrutiny of elected officials by the citizens

### To-do

* Crawling;
    * Download othe JSON files with relevant data from https://www.parlamento.pt/Cidadania/Paginas/DadosAbertos.aspx
    * Check if published DAR JSON files are as updated as the PDFs
        * if not, change the crawling to download the PDFs and convert them to text files
    * Create data pipelines (only download new data)
    * Make it automatically run periodically

* Data Preparation
    * Unzip all files
    * Load JSON files and extract relevant text
    * Fix text 
        * Remove html tags
        * replace special codes for non ascii characters

* Data Analysis
    * Create basic statistics
    * Explore the data

* Comunicate Results
    * Load data to Tableau
    * Create Dashboards
    * Publish Results on the Internet
