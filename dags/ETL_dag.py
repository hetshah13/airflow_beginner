from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import time
import os


# Define the scraping function
def scrape_policy_data():
    # Set up Selenium WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    try:
        # Open the Carrots & Sticks website
        driver.get("https://www.carrotsandsticks.net/policies/?region=Africa")
        
        # Allow time for the page to load
        time.sleep(5)

        # Click on the Africa filter
        africa_filter_button = driver.find_element(By.XPATH, '//*[@id="instruments"]/div/div[2]/div[1]/div/div[2]/div/label[1]/div')
        africa_filter_button.click()

        # Allow some time for filtering
        time.sleep(3)

        # Get the page source and parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Extract the policy data
        policy_list = soup.find_all('div', class_='instruments-result-container')
        policies = []

        # Loop through each policy and extract relevant details
        for policy in soup.find_all('div', class_='instruments-result'):
            title = policy.find('h3', class_='instruments-result-title').text.strip()  # Extract the title

            # Initialize variables
            country = ''
            year = ''
            policy_type = ''
            morv = ''

            # Loop through policy details
            for div in policy.find_all('div', class_='col-xs-12 col-sm-3'):
                label = div.find('span', class_='instruments-result-label').text.strip()
                value = div.get_text(strip=True).replace(label, '').strip()  # Clean the value

                # Map the label to the corresponding detail
                if label == 'Issuer':
                    country = value
                elif label == 'Year':
                    year = value
                elif label == 'Policy Type':
                    policy_type = value
                elif label == 'Mandatory or voluntary':
                    morv = value

            # Append the collected data for this policy
            policies.append([title, country, year, policy_type, morv])

        # Create a DataFrame and save it as a CSV file
        df = pd.DataFrame(policies, columns=['Title', 'Country', 'Year', 'Policy_Type', 'Mandatory or Voluntary'])
        df.to_csv('africa_policies.csv', index=False)

    finally:
        driver.quit()

with DAG(
    dag_id ='etl_policy_data',
    start_date = datetime(2024, 9, 30,5),
    description='An ETL DAG to scrape policy data',
    schedule_interval='@daily',
    ) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_policy_data',
        python_callable=scrape_policy_data
        )
    scrape_task
