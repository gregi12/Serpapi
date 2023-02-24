import base64
import pandas as pd
from serpapi import GoogleSearch
from google.cloud import bigquery
import datetime
import functions_framework
@functions_framework.cloud_event
def hello_pubsub(event_load):
    
    search_term = "data analyst"
    search_location = "United States"

    for num in range(45):

        start = num * 10
        params = {
            "api_key": "d9e1f7d32270c69ad8a9a1a657a16bc1fc98e3c32d6b99fd67c1d2167d66790b", #Fill in with your API key from SerpApi
            "device": "desktop",
            "engine": "google_jobs",
            "google_domain": "google.com",
            "q": search_term,
            "hl": "en",
            "gl": "us",
            "location": search_location,
            "chips": "date_posted:today",
            "start": start,
        }

        search = GoogleSearch(params)
        results = search.get_dict()

        # check if the last search page (i.e., no results)
        try:
            if results['error'] == "Google hasn't returned any results for this query.":
                break
        except KeyError:
            print(f"Getting SerpAPI data for page: {start}")
        else:
            continue

        # create dataframe of 10 pulled results
        jobs = results['jobs_results']
        jobs = pd.DataFrame(jobs)
        jobs = pd.concat([pd.DataFrame(jobs), 
                        pd.json_normalize(jobs['detected_extensions'])], 
                        axis=1).drop('detected_extensions', 1)
        jobs['date_time'] = datetime.datetime.utcnow()

        # concat dataframe
        if start == 0:
            jobs_all = jobs
        else:
            jobs_all = pd.concat([jobs_all, jobs])

        jobs_all['search_term'] = search_term
        jobs_all['search_location'] = search_location

    # send resluts to BigQuery
    table_id = "uk-jobs-project.Uk_jobs_listing.gsearch_jobs" # BigQuery Table name
    client = bigquery.Client()
    table = client.get_table(table_id)
    errors = client.insert_rows_from_dataframe(table, jobs_all)
    if errors == []:
        print("Data loaded into table")
        return "Success"
    else:
        print(errors)
        return "Failed"

    print("Hello, " + base64.b64decode(cloud_event.data["message"]["data"]).decode() + "!")