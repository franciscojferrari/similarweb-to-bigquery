import requests
import config as cfg
PARAMS = cfg.SIMILAR_WEB_API_CONFIG
import pandas as pd
import copy

def __getRequest(url, params):
    response = requests.get(url, params = params,  allow_redirects=False)
    # extracting data in json format 
    data = response.json()
    return data

def getDesktopVisits(dom):
    url = 'https://api.similarweb.com/v1/website/'+dom+'/traffic-and-engagement/visits'
    result = __getRequest(url, PARAMS)

    desktopVisits = []
    if "visits" in result:
        for dv in result['visits']:
            desktopVisits.append({'domain':dom, "date":dv['date'], "visits": dv['visits'], "device":"Desktop"})

    return pd.DataFrame(desktopVisits)

def getDesktopAverageVisitDuration(dom):
    url = 'https://api.similarweb.com/v1/website/'+dom+'/traffic-and-engagement/average-visit-duration'
    result = __getRequest(url, PARAMS)
    
    dataDesktopAvd = []
    if "average_visit_duration" in result:
        for dAvd in result['average_visit_duration']:
            dataDesktopAvd.append({'domain':dom, "date":dAvd['date'], "avg_duration":dAvd['average_visit_duration'], "device":"Desktop"})
    return pd.DataFrame(dataDesktopAvd)

def getDesktopPagePerVisit(dom):
    url = 'https://api.similarweb.com/v1/website/'+dom+'/traffic-and-engagement/pages-per-visit'
    result = __getRequest(url, PARAMS)
    
    dataDesktopPpv = []
    if "pages_per_visit" in result:
        for dPpv in result['pages_per_visit']:
            dataDesktopPpv.append({'domain':dom, "date":dPpv['date'], "page_per_visit":dPpv['pages_per_visit'], "device":"Desktop"})
    return pd.DataFrame(dataDesktopPpv)

def getDesktopBounceRate(dom):
    url = 'https://api.similarweb.com/v1/website/'+dom+'/traffic-and-engagement/bounce-rate'
    result = __getRequest(url, PARAMS)
    
    dataDesktopBr= []
    if "bounce_rate" in result:
        for dBr in result['bounce_rate']:
            dataDesktopBr.append({'domain':dom, "date":dBr['date'], "bounce_rate":dBr['bounce_rate'], "device":"Desktop"})
    return pd.DataFrame(dataDesktopBr)

def getMobileVisits(dom):
    url = 'https://api.similarweb.com/v2/website/'+dom+'/mobile-web/visits'
    result = __getRequest(url, PARAMS)

    mobileVisits = []
    if "visits" in result:
        for dv in result['visits']:
            mobileVisits.append({'domain':dom, "date":dv['date'], "visits": dv['visits'], "device":"Mobile"})

    return pd.DataFrame(mobileVisits)

def getMobileAverageVisitDuration(dom):
    url = 'https://api.similarweb.com/v2/website/'+dom+'/mobile-web/average-visit-duration'
    result = __getRequest(url, PARAMS)
    
    dataMobileAvd = []
    if "average_visit_duration" in result:
        for dAvd in result['average_visit_duration']:
            dataMobileAvd.append({'domain':dom, "date":dAvd['date'], "avg_duration":dAvd['average_visit_duration'], "device":"Mobile"})
    return pd.DataFrame(dataMobileAvd)

def getMobilePagePerVisit(dom):
    url = 'https://api.similarweb.com/v2/website/'+dom+'/mobile-web/pages-per-visit'
    result = __getRequest(url, PARAMS)
    
    dataMobilePpv = []
    if "pages_per_visit" in result:
        for dPpv in result['pages_per_visit']:
            dataMobilePpv.append({'domain':dom, "date":dPpv['date'], "page_per_visit":dPpv['pages_per_visit'], "device":"Mobile"})
    return pd.DataFrame(dataMobilePpv)

def getMobileBounceRate(dom):
    url = 'https://api.similarweb.com/v2/website/'+dom+'/mobile-web/bounce-rate'
    result = __getRequest(url, PARAMS)
    
    dataMobileBr= []
    if "bounce_rate" in result:
        for dBr in result['bounce_rate']:
            dataMobileBr.append({'domain':dom, "date":dBr['date'], "bounce_rate":dBr['bounce_rate'], "device":"Mobile"})
    return pd.DataFrame(dataMobileBr)

def getDesktopTrafficSources(dom):
    url = 'https://api.similarweb.com/v1/website/'+dom+'/traffic-sources/overview-share'
    result = __getRequest(url, PARAMS)

    dataDesktopTS = []
    dataDesktopTS_df = pd.DataFrame(dataDesktopTS)
    if "visits" in result:
        for visits in result['visits'][dom]:
            for visitSource in visits['visits']:
                dataDesktopTS.append({'domain' : dom, "date" : visitSource['date'], (visits['source_type'].replace(' ', '_').lower()) : (visitSource['organic'] + visitSource['paid']), "device" : "Desktop"})
        dataDesktopTS_df = pd.DataFrame(dataDesktopTS)
        dataDesktopTS_df = dataDesktopTS_df.groupby(['date', 'device','domain']).sum()
        dataDesktopTS_df = dataDesktopTS_df.reset_index()

    return dataDesktopTS_df

def getOrganicSearchKeywords(dom):
    url = 'https://api.similarweb.com/v1/website/'+dom+'/traffic-sources/organic-search'

    start_date = copy.deepcopy(PARAMS['start_date'])
    end_date = copy.deepcopy( PARAMS['end_date'])
    
    dates = pd.date_range(PARAMS['start_date'],PARAMS['end_date'], freq='MS').strftime("%Y-%m").tolist()
    dataSearchKeywords = []
    for date in dates:
        print(date + " - " + dom)
        PARAMS['start_date'] = date
        PARAMS['end_date'] = date
        PARAMS['limit'] = 1000
        result = __getRequest(url, PARAMS)
        if "search" in result:
            for search_term in result['search']:
                dataSearchKeywords.append({
                    'domain' : dom, 
                    "date" : date,
                    "search_term" : search_term['search_term'],
                    "share" : search_term['share'],
                    "visits" : search_term['visits'],
                    "url" : search_term['url'],
                    "position" : search_term['position']
                    })           

    PARAMS['start_date'] = start_date
    PARAMS['end_date'] = end_date   
    return pd.DataFrame(dataSearchKeywords)

def getKeywordsOrganicTrafficShare(keyword):
    url = 'https://api.similarweb.com/v1/keywords/'+keyword+'/analysis/organic-competitors'
    
    start_date = copy.deepcopy(PARAMS['start_date'])
    end_date = copy.deepcopy( PARAMS['end_date'])

    dates = pd.date_range(PARAMS['start_date'],PARAMS['end_date'], freq='MS').strftime("%Y-%m").tolist()
    dataKeywordsTrafficShare = []
    for date in dates:
        print(date + " - " + keyword)
        PARAMS['start_date'] = date
        PARAMS['end_date'] = date
        PARAMS['limit'] = 2000
        result = __getRequest(url, PARAMS)
        if "traffic_breakdown" in result:
            for domain in result['traffic_breakdown']:
                dataKeywordsTrafficShare.append({
                    "keyword" : keyword,
                    "date" : date,
                    'domain' : domain['domain'], 
                    "share" : domain['traffic_share'],
                    "destination_url" : domain['destination_url'],
                    "position" : domain['position'],
                    "website_categories": domain['website_categories']
                    }) 
    PARAMS['start_date'] = start_date
    PARAMS['end_date'] = end_date
    return pd.DataFrame(dataKeywordsTrafficShare)

def getWebsitePagesTrafficShare(dom):
    url = 'https://api.similarweb.com/v1/website/'+dom+'/website-content/popular-pages'

    start_date = copy.deepcopy(PARAMS['start_date'])
    end_date = copy.deepcopy( PARAMS['end_date'])
    
    dates = pd.date_range(PARAMS['start_date'],PARAMS['end_date'], freq='MS').strftime("%Y-%m").tolist()
    dataPagesShare = []
    for date in dates:
        print(date + " - " + dom)
        PARAMS['start_date'] = date
        PARAMS['end_date'] = date
        result = __getRequest(url, PARAMS)
        if "popular_pages" in result:
            for page in result['popular_pages']:
                dataPagesShare.append({
                    'domain' : dom, 
                    "date" : date,
                    "page" : page['page'],
                    "share" : page['share'],
                    })           

    PARAMS['start_date'] = start_date
    PARAMS['end_date'] = end_date   
    return pd.DataFrame(dataPagesShare)
