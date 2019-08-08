import requests
import json
import config as cfg
import similarweb_api.similarwebapi as sw
import pandas as pd
from functools import reduce
from google.oauth2 import service_account
import pandas_gbq
from google.cloud import bigquery
import datetime



def export_to_BQ():
    import base64
    websites = cfg.WEBSITES
    credentials = service_account.Credentials.from_service_account_info(cfg.GOOGLE_SERVICE_ACCOUNT)
    websites_desktop_mobile_data = []

    bq_client = bigquery.Client(credentials = credentials, project=cfg.GOOGLE_PROJECT)
    bq_datset = bq_client.dataset(cfg.BQ_DATESET)

    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)

    try:
        bq_client.get_table((cfg.BQ_DATESET + "." + cfg.BQ_TABLE))
        print("Changing the starting date to last month")
        cfg.SIMILAR_WEB_API_CONFIG['start_date'] = lastMonth.strftime("%Y-%m")
        cfg.SIMILAR_WEB_API_CONFIG['end_date'] = lastMonth.strftime("%Y-%m")

    except:
        print("Executing for first time. Using start time")
        
        cfg.SIMILAR_WEB_API_CONFIG['end_date'] = lastMonth.strftime("%Y-%m")

    # Checks if df have the correct columsn and are not empty
    def checkcolumns(left, right):
        columns = ['date', 'device','domain']
        if set(columns).issubset(left.columns):
            if set(columns).issubset(right.columns):
                return pd.merge(left,right,on=columns, how='inner')
            else:
                return left
        return right

    for _ in websites:
        webiste = _['domain']
        category = _['category'] 
    
        desktopVisits = sw.getDesktopVisits(webiste)
        desktopAverageVisitDuration = sw.getDesktopAverageVisitDuration(webiste)
        desktopPagePerVisit = sw.getDesktopPagePerVisit(webiste)
        desktopBounceRate = sw.getDesktopBounceRate(webiste)
        desktopTrafficSources = sw.getDesktopTrafficSources(webiste)

        mobileVisits = sw.getMobileVisits(webiste)
        mobileAverageVisitDuration = sw.getMobileAverageVisitDuration(webiste)
        mobilePagePerVisit = sw.getMobilePagePerVisit(webiste)
        mobileBounceRate = sw.getMobileBounceRate(webiste)

        desktop_df =[desktopVisits, desktopAverageVisitDuration, desktopPagePerVisit, desktopBounceRate, desktopTrafficSources]

        desktopData = reduce(lambda  left,right: checkcolumns(left, right), desktop_df)
        
        mobile_df = [mobileVisits, mobileAverageVisitDuration, mobilePagePerVisit, mobileBounceRate]
        mobileData = reduce(lambda  left,right: checkcolumns(left, right), mobile_df)
        
        desktop_mobile_data = pd.concat([desktopData, mobileData], ignore_index=True, sort=False)
        desktop_mobile_data['category'] = category

        websites_desktop_mobile_data.append(desktop_mobile_data)

    websites_desktop_mobile_data = pd.concat(websites_desktop_mobile_data)
    pandas_gbq.to_gbq(
        websites_desktop_mobile_data,
        (cfg.BQ_DATESET + "." + cfg.BQ_TABLE), 
        if_exists="append", 
        credentials=credentials,
        project_id = cfg.GOOGLE_PROJECT)
    print("Number of row inserted {}".format(websites_desktop_mobile_data.shape))

def export_keywords_to_BQ():
    import base64
    
    websites = cfg.WEBSITES
    credentials = service_account.Credentials.from_service_account_info(cfg.GOOGLE_SERVICE_ACCOUNT)
    websites_keywords_data = []

    bq_client = bigquery.Client(credentials = credentials, project=cfg.GOOGLE_PROJECT)

    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)

    try:
        bq_client.get_table((cfg.BQ_DATESET + "." + cfg.BQ_TABLE_KEYWORDS))
        print("Changing the starting date to last month")
        cfg.SIMILAR_WEB_API_CONFIG['start_date'] = lastMonth.strftime("%Y-%m")
        cfg.SIMILAR_WEB_API_CONFIG['end_date'] = lastMonth.strftime("%Y-%m")

    except:
        print("Executing for first time. Using start time")
        cfg.SIMILAR_WEB_API_CONFIG['end_date'] = lastMonth.strftime("%Y-%m")

    for _ in websites:
        webiste = _['domain']
        category = _['category'] 
        keywords_data = sw.getOrganicSearchKeywords(webiste)
        keywords_data['category'] = category

        websites_keywords_data.append(keywords_data)
        
    websites_keywords_data = pd.concat(websites_keywords_data)
    pandas_gbq.to_gbq(
        websites_keywords_data,
        (cfg.BQ_DATESET + "." + cfg.BQ_TABLE_KEYWORDS), 
        if_exists="append", 
        credentials=credentials,
        project_id = cfg.GOOGLE_PROJECT)
    print("Number of row inserted {}".format(websites_keywords_data.shape))

def export_keyword_trafficshare_to_BQ():
    import base64
    
    keywords = cfg.KEYWORDS
    credentials = service_account.Credentials.from_service_account_info(cfg.GOOGLE_SERVICE_ACCOUNT)
    keywords_trafficshare_data = []

    bq_client = bigquery.Client(credentials = credentials, project=cfg.GOOGLE_PROJECT)

    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)

    try:
        bq_client.get_table((cfg.BQ_DATESET + "." + cfg.BQ_TABLE_KEYWORDS_TRAFFICSHARE))
        print("Changing the starting date to last month")
        cfg.SIMILAR_WEB_API_CONFIG['start_date'] = lastMonth.strftime("%Y-%m")
        cfg.SIMILAR_WEB_API_CONFIG['end_date'] = lastMonth.strftime("%Y-%m")

    except:
        print("Executing for first time. Using start time")
        cfg.SIMILAR_WEB_API_CONFIG['end_date'] = lastMonth.strftime("%Y-%m")

    for _ in keywords:
        keyword = _['keyword']
        # category = _['category'] 
        keywords_data = sw.getKeywordsOrganicTrafficShare(keyword)
        # keywords_data['category'] = category

        keywords_trafficshare_data.append(keywords_data)
        
    keywords_trafficshare_data = pd.concat(keywords_trafficshare_data)
    pandas_gbq.to_gbq(
        keywords_trafficshare_data,
        (cfg.BQ_DATESET + "." + cfg.BQ_TABLE_KEYWORDS_TRAFFICSHARE), 
        if_exists="append", 
        credentials=credentials,
        project_id = cfg.GOOGLE_PROJECT)
    print("Number of row inserted {}".format(keywords_trafficshare_data.shape))

def export_pages_trafficshare_to_BQ():
    import base64
    
    websites = cfg.WEBSITES
    credentials = service_account.Credentials.from_service_account_info(cfg.GOOGLE_SERVICE_ACCOUNT)
    websites_keywords_data = []

    bq_client = bigquery.Client(credentials = credentials, project=cfg.GOOGLE_PROJECT)

    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)

    try:
        bq_client.get_table((cfg.BQ_DATESET + "." + cfg.BQ_TABLE_PAGES_TRAFFICSHARE))
        print("Changing the starting date to last month")
        cfg.SIMILAR_WEB_API_CONFIG['start_date'] = lastMonth.strftime("%Y-%m")
        cfg.SIMILAR_WEB_API_CONFIG['end_date'] = lastMonth.strftime("%Y-%m")

    except:
        print("Executing for first time. Using start time")
        cfg.SIMILAR_WEB_API_CONFIG['end_date'] = lastMonth.strftime("%Y-%m")

    for _ in websites:
        webiste = _['domain']
        category = _['category'] 
        keywords_data = sw.getWebsitePagesTrafficShare(webiste)
        keywords_data['category'] = category

        websites_keywords_data.append(keywords_data)
        
    websites_keywords_data = pd.concat(websites_keywords_data)
    pandas_gbq.to_gbq(
        websites_keywords_data,
        (cfg.BQ_DATESET + "." + cfg.BQ_TABLE_PAGES_TRAFFICSHARE), 
        if_exists="append", 
        credentials=credentials,
        project_id = cfg.GOOGLE_PROJECT)
    print("Number of row inserted {}".format(websites_keywords_data.shape))

def run_all_functions(event, context):
    export_to_BQ()
    export_keywords_to_BQ()
    export_keyword_trafficshare_to_BQ()
    export_pages_trafficshare_to_BQ()

