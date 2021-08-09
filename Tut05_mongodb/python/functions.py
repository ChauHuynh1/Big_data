'''
Import necessary libraries
'''
import re
import csv
import os
from sys import stdout
from time import sleep
from time import time
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup as soup
from tqdm import tqdm

'''
Function to request page html from given URL
'''
def page_html(requested_url):
    try:
        # define headers to be provided for request authentication
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) ' 
                        'AppleWebKit/537.11 (KHTML, like Gecko) '
                        'Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}
        # make request, read request object to get page html and return it.
        request_obj = Request(url = requested_url, headers = headers)
        opened_url = urlopen(request_obj)
        page_html = opened_url.read()
        opened_url.close()
        return page_html
    except Exception as e:
        print(e)
        pass

'''
Function to acquire the maximum number of jobs (only applicable for the base/ first html)
'''
def max_num_jobs(page_html):
    page_soup = soup(page_html, "html.parser")
    max_ = page_soup.find("h1", {"data-test": "jobCount-H1title"})
    return(max_.get_text())

'''
Function to return a list of job page links from a given html page
'''
def get_listing_links(page_html):
    try:
        # use of dictionary to make sure that there are no duplicates
        obj_links = {}
        id_temp_dict = {}
        page_soup = soup(page_html, "html.parser")
        # grab all divs with a class of result
        results = page_soup.findAll("ul", {"data-test": "jlGrid"})
        for result in results:
            # print("result", result)
            links = result.findAll('a')
            # print("links", links)
            for a in links:
                formatted_link = "http://www.glassdoor.sg" + a['href']
                id_temp = formatted_link[-10:]
                if id_temp not in id_temp_dict.keys():
                    id_temp_dict[id_temp] = None
                    obj_links[formatted_link] = None
        return list(obj_links.keys())
    except Exception as e:
        print(e)
        return []

'''
Function to return a dictionary of scrapped information from a single job page link 
'''
def jobpage_scrape(extracted_link, page_html, career):
    jobpage_info = {}
    page_soup = soup(page_html, "html.parser")
    try:
        jobpage_info['job_link'] = extracted_link
    except Exception as e:
        print('/n','Error get link', e, extracted_link)
        jobpage_info['job_link'] = None

    try:
        job_title = page_soup.find("div", {"class": "css-17x2pwl e11nt52q6"})
        jobpage_info['job_title'] = job_title.get_text()
    except Exception as e:
        print('/n','Error get title', e, extracted_link)
        jobpage_info['job_title'] = None

    try:
        j_d = page_soup.find("div", {"class": "desc css-58vpdc ecgq1xb4"})
        job_desc = j_d.get_text()
        pattern = '\n' + '{2,}'
        job_desc = re.sub(pattern, '\n', job_desc)
        job_desc = job_desc.replace('\n', " ")
        jobpage_info['job_description'] = job_desc        
    except Exception as e:
#         print('/n','Error get desc', e, extracted_link)
        jobpage_info['job_description'] = None
    
    jobpage_info['career'] = career

    return jobpage_info

'''
Function to write a dictionary of scrapped information onto a csv file
'''
def write_to_file(filename, jobpage_info):
    with open(filename, 'a', newline='', encoding="utf-8") as f:
        try:
            writer = csv.writer(f)
            writer.writerow(jobpage_info.values())
        except Exception as e:
            print(e)
            pass

'''
Function to combine above and scrap from a single search url
'''

def scrap_list_from_url(base_url, keyword):
    run = True
    page_num = 1
    job_num = 0
    base_html = page_html(base_url)

    while run:
        new_url = base_url + '&p={}'.format(page_num)
        base_html = page_html(new_url)
            
        toc = time()
        extracted_links = get_listing_links(base_html)

        if len(extracted_links) == 0:
            print("Done scraping this url ", base_url)
            with open("log.txt", 'a', newline='', encoding="utf-8") as f:
                try:
                    f.writelines(new_url)
                    run = False
                    continue
                except Exception as e:
                    print(e)
                    pass

        tic = time()

        print("Found {} job links in page {}: {}". format(len(extracted_links), page_num, new_url))
        print("Time taken to extract page links: {}".format(tic - toc))
        print("Starting ...\n")

        i = 0
        toc = time()
        current_scrape = []
        for extracted_link in extracted_links:
            scraped_html = None
            scraped_html = page_html(extracted_link)

            if scraped_html != None:
                job_num = job_num + 1
                jobpage_info = jobpage_scrape(extracted_link, scraped_html, keyword)
                current_scrape.append(jobpage_info)
                i = i + 1
                stdout.write("\rPage scrape progress: %d/ %d" % (i, len(extracted_links)))
                stdout.flush()
                        
            else:
                print("Error detected in", extracted_link)
                continue
        tic = time()
        print('\nTime taken to scrape page: {}\n'. format(tic - toc))
#         print("Total jobs scraped: {}\n".format(job_num))
        if run == True:
#             print("Moving onto next page...")
            page_num = page_num + 1
        return current_scrape


def scrap_from_url(keyword):
    base_url = "https://www.glassdoor.com/Job/jobs.htm?sc.keyword=" + keyword.replace(" ", "%20")
#     current_run = scrap_list_from_url(base_url + "&locT=N&locId=251", keyword)
    current_scrape = scrap_list_from_url(base_url + "&locId=16&locT=N&locName=Australia", keyword)
    return current_scrape
        
'''
Simple skills NER with DBPedia Spotlight API
'''

import spacy_dbpedia_spotlight
nlp = spacy_dbpedia_spotlight.create('en')

def dbpedia_ner_job(job_data):
    text_string = job_data['job_description']
    try:
        doc = nlp(text_string)
        ent_list = []
        for ent in doc.ents:
            ent_list.append(ent.text)
        ent_list = list(set(ent_list))
        job_data['entities'] = ent_list
    except Exception as e:
        job_data = None
        pass            
    return job_data