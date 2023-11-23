# glass-dome
Tools to improve the scrutiny of elected officials by the citizens

### To-do

* Crawling:
    * Create data pipelines (only download new data)
        * make AR_downloader only download new files
    * Make it automatically run periodically
        * Integrate the crawling and download in and Airflow dag

* Data Preparation:
    * Load JSON files
    * Clean data
        * Remove html tags
        * replace special codes for non ascii characters

* Data Analysis:
    * Create basic statistics
    * Explore the data

* Comunicate Results:
    * Load data to Tableau
    * Create Dashboards
    * Publish Results on the Internet
