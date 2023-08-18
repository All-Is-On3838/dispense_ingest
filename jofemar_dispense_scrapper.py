## for automated extraction and upload of daily jofemar dispenses
## Created by Allison Li 03/07/2023
## Modified by...
## Allison 04/18/2023 updated to match georgia's team's manual data cleaning process (added department)
##

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import pytz
import time
import os
import logging
import pandas as pd
import sys
from google.cloud import bigquery, storage
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

class scrapper():

    def __init__(self, user, pswd, month_index, year, day):
        # #set download folder preferences
        download_location = "/usr/app/dispense_csv"
        # download_location = os.getcwd()+"\dispense_csv"
        self.download_location = download_location
        logger.info(f"download location: {download_location}")

        # set chromedriver options
        options = webdriver.ChromeOptions()
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_argument('--enable-precise-memory-info')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-default-apps')
        options.add_argument('--no-sandbox') # required to run in docker
        #options.add_argument('--headless') # don't need to open window
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--test-type')
        prefs = {'download.default_directory': f'{download_location}',
                 "download.prompt_for_download": False,
                 "download.directory_upgrade": True}
        options.add_experimental_option('prefs', prefs)

        # Connect to the remote WebDriver server and create a new Chrome driver instance
        self.driver = webdriver.Chrome(options=options)

        #credentials
        self.user = user
        self.pswd = pswd

        #date
        self.month_index = month_index
        self.year = year
        self.day = day

        #open jofemar login page
        self.driver.get("https://jsuite.jofemar.com/LogInUser.aspx")

    def teardown_method(self):
        self.driver.quit()

    def login(self):
        self.driver.find_element(By.ID, "cphBody_logViewLogin_logPreInicioSesion_UserName").click()
        self.driver.find_element(By.ID, "cphBody_logViewLogin_logPreInicioSesion_UserName").send_keys(self.user)
        self.driver.find_element(By.ID, "cphBody_logViewLogin_logPreInicioSesion_btnAceptar").click()
        self.driver.find_element(By.ID, "cphBody_logViewLogin_logInicioSesion_Password").send_keys(self.pswd)
        self.driver.find_element(By.ID, "cphBody_logViewLogin_logInicioSesion_btnAceptar").click()
    
    def open_dispense_report(self):
        #open dispense report page
        element = self.driver.find_element(By.XPATH, '//*[@id="BarraMenu1_menu:submenu:17"]/li[1]/a')
        if element:
            dispense_report_url = element.get_attribute("href")
            logger.debug(f'pulling data from url: {dispense_report_url}')
        #self.driver.implicitly_wait(1)
        self.driver.get(dispense_report_url)

    def choose_date(self, date_picker):
        try:
            if date_picker == "start":
                date_picker = self.driver.find_element(By.CSS_SELECTOR, "#cphBody_ctl01_ctl00_divDateDesde > .ui-datepicker-trigger")
            elif date_picker == "end":
                date_picker = self.driver.find_element(By.CSS_SELECTOR, "#cphBody_ctl01_ctl00_divDateHasta > .ui-datepicker-trigger")
        except:
            logger.exception("invalid date picker, check scrapper.choose_date")
        #activate date picker
        date_picker.click()
        
        #select month
        select_month = self.driver.find_element(By.CSS_SELECTOR, ".ui-datepicker-month")
        select_index = Select(select_month)
        select_index.select_by_value(f'{self.month_index}')

        #select year
        select_year = self.driver.find_element(By.CSS_SELECTOR, ".ui-datepicker-year")
        select_index = Select(select_year)
        select_index.select_by_value(f'{self.year}')

        #select date
        start_date = self.driver.find_element(By.LINK_TEXT, f'{self.day}')
        start_date.click()
        
    def validate_date(self, yesterday):
        QC = "date not checked yet"
        
        start_date = self.driver.find_element(By.ID, "Dispensaciones_CtrlPanelFiltros_RangoFechas_txtFechaInicio")
        start_value = start_date.get_attribute("value")
        end_date = self.driver.find_element(By.ID, "Dispensaciones_CtrlPanelFiltros_RangoFechas_txtFechaFin")
        end_value = end_date.get_attribute("value")

        if start_value == yesterday:
            QC = "pass"
        else:
            QC = "incorrect start date, check logs"
            logger.debug(f'QC_date: {yesterday}, start value: {start_value}')
            self.driver.quit()

        if end_value == yesterday:
            QC = "pass"
        else:
            QC = "incorrect end date, check logs"
            logger.debug(f'QC_date: {yesterday}, end value: {end_value}')
            self.driver.quit()
        logger.debug(f"Validate date QC status: {QC}")

    def refresh_filters(self):
        filter = self.driver.find_element(By.ID, "cphBody_ctl01_btnAplicarFiltros")
        if "WebForm" in filter.get_attribute("onclick"):
            filter.click()
        record_count = self.driver.find_element(By.CSS_SELECTOR, "#divPaginadorDispensaciones_DivGrd_right > .ui-paging-info")
        #make sure table is loaded before moving on
        timeout = 0
        if record_count.text == "No records to view" and timeout < 10:
            timeout += 1
            time.sleep(1)
        elif record_count.text == "No records to view" and timeout >= 10:
            print("Could not get table, timeout met")
            self.driver.quit()
        else:
            logger.debug(f'Checking table was refreshed after selecting date. Table contains: {record_count.text}')

    def download_csv(self, dl_date):
        elements = self.driver.find_elements(By.CSS_SELECTOR, ".ui-pg-button-text")
        dl_file_name = f'Movements {dl_date}.csv'
        for file_button in elements:
            if file_button.text == "CSV ": #Allison 03/03/2023 leave the space, the space is in the html
                logger.debug(f"Clicking button: {file_button.text}")
                file_button.click()   

        #wait for download to complete
        logger.info(f"Checking {self.download_location} for {dl_file_name}")
        logger.info(f"Download location files: {os.listdir(self.download_location)}")
        seconds = 0
        while seconds < 30:
            for fname in os.listdir(self.download_location):
                if fname == dl_file_name:
                    logger.info(f"Downloaded file matched. {dl_file_name} was downloaded at {self.download_location}")
                    seconds = 30
                    break
            logger.info(f"Download not complete, add 1 second. Looking for {dl_file_name}. Tried for {seconds} seconds.")
            logger.info(f"Directory currently hold: {os.listdir(self.download_location)}")
            time.sleep(1)
            seconds += 1

        #log browser logs
        logs = self.driver.get_log('browser')
        for log in logs:
            if 'download' in log['message']:
                logger.info(f"----------------chromedriver log: {log['message']}")

        return f"{self.download_location}/{dl_file_name}"


def csv_to_dataframe(dl_file):
    #read csv
    return pd.read_csv(dl_file, sep=';')

def transform_dataframe(df):
    #keep needed columns
    cols_to_keep = ['Machine Id', 'Machine', 'User', 'Employee number', 'Identifier', 'Location', 'Department', 'Product', 'Date', 'Cost price','Type', 'Cost Center']
    clean_df = df.loc[:, cols_to_keep]

    #remove rejected dispenses
    rows_to_drop = []
    for i,r in clean_df.iterrows():
        if str(clean_df.loc[i,'Employee number']) == 'nan':
            rows_to_drop.append(i)
        elif str(clean_df.loc[i,'Department']) == 'TestAlvaro':
            rows_to_drop.append(i)
        elif str(clean_df.loc[i,'Product']) =='CREDIT UPDATE FROM EXTERNAL SYSTEM':
            rows_to_drop.append(i)
    clean_df = clean_df.drop(index=rows_to_drop)


    #rename columns
    clean_df = clean_df.rename(columns={'Machine Id': 'MACHINE NUMBER',
                                        'Machine': 'DISPENSER',
                                        'User': 'USER NAME',
                                        'Employee number': 'EMPLOYEE NUMBER',
                                        'Identifier': 'CARD',
                                        'Location': 'LOCATION',
                                        'Department': 'DEPARTMENT',
                                        'Product': 'PRODUCT',
                                        'Date': 'DATE',
                                        'Cost price': 'PRICE',
                                        'Type': 'TYPE',
                                        'Cost Center': 'COST CENTER'})
    
    data_types_dict = { 'MACHINE NUMBER': 'Int64',
                        'DISPENSER': 'string',
                        'USER NAME': 'string',
                        'EMPLOYEE NUMBER': 'string',
                        'CARD': 'string',
                        'LOCATION': 'string',
                        'DEPARTMENT': 'string',
                        'PRODUCT': 'string',
                        'DATE': 'string',
                        'PRICE': 'Int64',
                        'TYPE': 'string',
                        'COST CENTER': 'string',
                        #'PERSON CODE': 'Int64',  #Allison 03/03/23 will not convert because all null values
                        #'PREVIOUS CREDIT': 'float64' #Allison 03/03/23 will not convert because all null values
                        }
    clean_df = clean_df.astype(data_types_dict)

    #add columns not provided in new jofemar site to maintain bigquery schema
    clean_df['PREVIOUS CREDIT'] = ''
    clean_df['PERSON CODE'] = ''

    return clean_df

def check_ready_for_upload(df):
    status = "have not checked yet"

    #check column names
    list(df)
    column_format = ['MACHINE NUMBER', 'DISPENSER', 'USER NAME', 'EMPLOYEE NUMBER', 'CARD', 'LOCATION', 'DEPARTMENT', 'PRODUCT', 'DATE', 'PRICE', 'TYPE', 'COST CENTER', 'PREVIOUS CREDIT', 'PERSON CODE']
    if list(df) == column_format:
        status = "ready for upload"
    else:
        status = f"not ready, check column names: {list(df)}"

    #check column data types
    table_column_dtypes = df.dtypes.tolist()
    correct_dtypes = "[Int64Dtype(), string[python], string[python], string[python], string[python], string[python], string[python], string[python], string[python], Int64Dtype(), string[python], string[python], dtype('O'), dtype('O')]"
    if f'{table_column_dtypes}' == correct_dtypes:
        status = "ready for upload"
    else:
        status = f"check column data types: {table_column_dtypes}"
    
    return status

def dataframe_to_csv(df, bq_date):
    file_path = f'dispense_csv/jofemar_dispense_{bq_date}.csv'
    df.to_csv(file_path, index=False)
    return file_path


def upload_csv(file_path):
    table_id = 'lts-palantir-hhs-exchange.kiosk.dispense_webscraper_temp'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'jofemar_webscrapper_service_account.json'
    client = bigquery.Client()
    load_job_configuration = bigquery.LoadJobConfig()
    load_job_configuration.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    load_job_configuration.schema = [
        bigquery.SchemaField('machine_number', 'INT64', mode='NULLABLE'),
        bigquery.SchemaField('dispenser', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('user_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('employee_number', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('card', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('location', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('department', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('product', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('date', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('price', 'INT64', mode='NULLABLE'),
        bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('cost_center', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('previous_credit', 'FLOAT64', mode='NULLABLE'),
        bigquery.SchemaField('person_code', 'INT64', mode='NULLABLE')
    ]

    # load_job_configuration.autodetect = True #schema provided above
    load_job_configuration.source_format = bigquery.SourceFormat.CSV
    load_job_configuration.skip_leading_rows = 1
    load_job_configuration.allow_quoted_newlines = True

    with open(file_path, 'rb') as source_file:
        upload_job = client.load_table_from_file(
            source_file,
            destination=table_id,          
            location='us-central1',
            job_config=load_job_configuration
        )

def convert_filetime(dateString):
    date_time_obj = datetime.strptime(dateString, '%Y-%m-%d %H:%M:%S.%f')
    return date_time_obj.strftime("%Y%m%d_%H_%M_%S_%f")

def error_email(note):
    message = Mail(
        from_email='Results@testandgo.com',
        to_emails= 'ali@lts.com',
        subject='Jofemar Dispense Daily Append Error',
        html_content=f'''
                    <p>Issue with Jofemar Dispense Daily Append code has occured. {note} Please check logs.</p>
                    '''
    )
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        logger.debug(response.status_code)
        logger.debug(response.body)
        logger.debug(response.headers)
    except Exception as e:
        logger.exception(e.message)

def upload_logs(file):
    #define configurations
    bucket_name = 'jofemar-dispense'
    upload_file = file

    #Upload to Google cloud storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(upload_file)
    blob.upload_from_filename(upload_file)

#load login credentials
load_dotenv()
user = os.environ.get("USER")
pswd = os.environ.get("PSWD")


#load credentials
load_dotenv()
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")

#set starttime for logger
startTime = convert_filetime(str(datetime.now()))

#set date for Jofemar lookup
current_dateTime = datetime.now(pytz.timezone("America/New_York"))
yesterday = current_dateTime - timedelta(days=1)
month_index = (yesterday.month - 1)
year = yesterday.year
day = yesterday.day

#set date for bigquery upload
bq_date = yesterday.strftime("%m%d%Y")

#set date for download lookup
dl_date = current_dateTime.strftime('%d%m%Y') #jofemar names the file based on the day the file was downloaded i.e. today

#set date for date validation
qc_date = yesterday.strftime('%m/%d/%Y')



if __name__ == "__main__" :
    #set logger
    logger = logging.getLogger(__name__) 
    logger.setLevel(logging.DEBUG)

    filehandler_name = f"\logs\{startTime}_Jofemar_Dispense_Append.log"
    formatter = logging.Formatter('%(levelname)s %(filename)s %(asctime)s %(message)s')
    filehandler = logging.FileHandler(filename=filehandler_name)
    # # # set output to Docker Logs/terminal
    # filehandler = logging.StreamHandler(sys.stdout)
    filehandler.setFormatter(formatter)

    logger.addHandler(filehandler)

    logger.info('***************************************')
    logger.info('New Program Run for daily Jofemar dispense append to bigquery table')
    logger.info("Process started at: " + startTime)
    logger.info('***************************************')

    #0 open browser to https://jsuite.jofemar.com/LogInUser.aspx
    dispense_file = scrapper(user, pswd, month_index, year, day)

    #1 log in
    dispense_file.login()

    #2 go to dispense report
    dispense_file.open_dispense_report()
    
    #3 set dates to today
    dispense_file.choose_date("start")
    dispense_file.choose_date("end")
    dispense_file.validate_date(qc_date)

    #4 refresh filters
    dispense_file.refresh_filters()

    #5 download csv
    download_location = dispense_file.download_csv(dl_date)

    #6 exit session
    dispense_file.teardown_method()

    #7 upload csv to bigquery
    #get file ready for upload
    upload_file_df = csv_to_dataframe(download_location)
    clean_df = transform_dataframe(upload_file_df)
    
    #upload if df passes qc
    if check_ready_for_upload(clean_df) == "ready for upload":
        file_path = dataframe_to_csv(clean_df, bq_date)
        upload_csv(file_path)
    else:
        logger.info("check df ready for upload")
        error_email("DATAFRAME NOT READY TO APPEND.")

    logger.info('***************************************')
    logger.info('Completed program run for daily Jofemar dispense append to bigquery table')
    logger.info('***************************************')

    #8 upload logs to cloud storage
    upload_logs(filehandler_name)