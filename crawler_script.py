#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import logging
from urllib.parse import urljoin
from lxml import etree
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


def wait(maximum_time, condition=None):
    """
    Specify the maximum time to wait
    if this time is exceeded and all elements have not been loaded
    a timeout exception is captured
    """
    try:
        wait = WebDriverWait(driver, maximum_time)
        if condition is not None:
            wait.until(condition)
            logging.info('condition %s successfully!', condition)
    except TimeoutException as e:
        logging.error('Failed to load all elements in defined %s time!', maximum_time)


def get_url(input_url):
    input_url = urljoin('https://hk.jobsdb.com/', input_url)
    return input_url


def get_html(driver, url, maximum_time, condition):
    logging.info('scraping %s...', url)
    driver.get(url)
    wait(maximum_time, condition)
    driver.get_screenshot_as_file('preview.png')
    html = driver.page_source
    if len(html) == 0:
        logging.info('The source codes of %s have not been loaded', url)
    else:
        return html


def parse_pages(html):
    """
    Parsing the results of the search page
    """
    html = etree.HTML(html)
    positionUrl = html.xpath('//article//h1/a[@target = "_top"]/@href')
    position = html.xpath('//article//h1/a[@target = "_top"]//text()')
    jobCompanyLink = html.xpath('//article//a[@data-automation = "jobCardCompanyLink"]/@href')
    jobCompany = html.xpath('//article//a[@data-automation = "jobCardCompanyLink"]//text()')
    jobLocationLink = html.xpath('//article//a[@data-automation = "jobCardLocationLink"]/@href')
    jobLocation = html.xpath('//article//a[@data-automation = "jobCardLocationLink"]//text()')
    launchTime = html.xpath('//time/@datetime')
    page_dict = {
        'positionUrl': positionUrl,
        'position': position,
        'jobCompanyLink': jobCompanyLink,
        'jobCompany': jobCompany,
        'jobLocationLink': jobLocationLink,
        'jobLocation': jobLocation,
        'launchTime': launchTime
    }
    return page_dict


def parse_detail(html):
    """
    Parsing results from detail pages
    """
    html = etree.HTML(html)
    jobHighlights = html.xpath('//div[@data-automation = "job-details-job-highlights"]//li//text()')
    jobDescription = html.xpath('//div[@data-automation = "jobDescription"]//p//text()')
    additionalInfo = html.xpath('//*[@id="contentContainer"]/div/div/div[2]/div/div[1]/div/div[3]/div/div[2]//text()')
    applyLink = html.xpath('//a[@data-automation="applyNowButton"]/@href')
    detail_dict = {
        'jobHighlights': jobHighlights,
        'jobDescription': jobDescription,
        'additionalInfo': additionalInfo,
        'applyLink': applyLink
    }
    return detail_dict


def save_data(df, data):
    for item in data.keys():
        df.insert(df.shape[1], item, data.get(item))
    return df


if __name__ == '__main__':
    # Set the logging format
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

    # Setting up the browser as a headless browser
    ch_options = Options()
    ch_options.add_argument('--headless')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=ch_options)
    driver.set_window_size(1366, 768)

    expected_url = "https://hk.jobsdb.com/hk/search-jobs"
    driver.get(expected_url)
    actual_url = driver.current_url

    # check the success of the home page
    if actual_url == expected_url:
        logging.info('scraping %s...', actual_url)
    else:
        logging.info('scraping %s..., but expected_url is %s', actual_url, expected_url, exc_info=True)

    driver.get_screenshot_as_file('preview.png')

    # Set a wait time in case the interactive element has not finished loading
    # and is thus not interactive
    wait(10, EC.element_to_be_clickable((By.XPATH, '//button[@data-automation="searchSubmitButton"]')))

    # Search for jobs with keywords-green finance research assistant
    input = driver.find_element(By.XPATH, '//input[@data-automation="searchKeywordsField"]')
    input.send_keys("green finance research assistant")
    button = driver.find_element(By.XPATH, '//button[@data-automation="searchSubmitButton"]')
    button.click()

    wait(10, EC.presence_of_all_elements_located((By.XPATH, '//article')))
    driver.get_screenshot_as_file('preview.png')

    # Parse the search page
    logging.info('parsing %s...', driver.current_url)
    dataPageInfo = parse_pages(driver.page_source)

    # Parse the detail page
    for item in dataPageInfo['positionUrl']:
        url = get_url(item)
        html = get_html(driver, url, 10, EC.presence_of_all_elements_located((By.XPATH, '//*[@id="contentContainer"]')))
        logging.info('parsing %s...', item)
        dataDetailTmp = parse_detail(html)
        for item in dataDetailTmp.keys():
            if item not in dataPageInfo.keys():
                dataPageInfo[item] = [dataDetailTmp[item]]
            else:
                dataPageInfo.get(item).append(dataDetailTmp.get(item))

    # Save data as a dataFrame
    dfRaInfo = pd.DataFrame()
    dfRaInfo = save_data(dfRaInfo, dataPageInfo)

    # Set the dataframe display format
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
