# KOFScraper

Web scraper built for KOF's internal use, based on ARGUS and Scrapy.

## Getting started

**Note for Windows users**: This was developed using Linux, most of it should be fine but if you encounter some errors you're gonna have to diagnose them youselves.
The kill_all script is in bash so that will not work. I think ChatGPT will translate it to powershell without many problems, otherwise consider installing WSL and join the dark side...

Firstly, clone the repository
```sh
git clone https://github.com/MattiaSalve/KOFScraper.git
cd KOFScraper
```
Create a virtual environment and install requirements
 ```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mkdir chunks
 ```
Now open a new terminal window, cd into the KOFScraper directory, source the virtual environment and start a scrapy server by running 
 ```sh
scrapyd
 ```
Go back to the terminal window in which you were installing KOFScraper and run 
 ```sh
scrapyd-deploy default -p ARGUS
 ```
This command compiles any changes to the program and should be run every time something is changed to the program.

## Running the scraper
Once the scraper is installed make sure that you are into the root of the project's directory and that scrapyd is running, then 
 ```sh
source .venv/bin/activate
python3 ARGUR_noGUI.py input_file.csv
 ```
Change input_file.csv with the list of urls, a csv file with two columns: "BVD" and "url"

# Output
The output will be saved under chunks/run_id=YYYY-MM-DD, which will contain two folders named "parsed" and "raw_html". 
Parsed is the folder containing all data as parquet files and raw_html contains the compressed raw html

# Modifying the scraper
The main parts of the scraper are:
- bin: contains the main libraries that are called throughout the project
- ARGUS/settings.py: Settings for the scraper, mostly about concurrency
- ARGUS/pipelines.py: The pipeline used by Scrapy, basically this tells Scrapy what to do after the page is scraped, such as how to structure the table, what informations to save etc.
- ARGUS/spiders/dualspider.py: Settings for the spider, handling sublinks following, text parsing, request starting and error handling

Reminder: After changing code remember to run 
 ```sh
scrapyd-deploy default -p ARGUS
 ```
