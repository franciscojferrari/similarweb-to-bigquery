#!/usr/bin/env python

SIMILAR_WEB_API_CONFIG = {
    "api_key": "",
    "country": "world",
    "start_date": "2018-01",
    "end_date": "2019-05",
    "granularity": "daily",
    "main_domain_only": "false",
    "show_verified": "false",
    "format": "json"
}

GOOGLE_SERVICE_ACCOUNT = {
    # Service account JSON
  }

BQ_DATESET = "SimilarWeb"
BQ_TABLE = "similarweb_export"
BQ_TABLE_KEYWORDS = "similarweb_keywords_export"
BQ_TABLE_KEYWORDS_TRAFFICSHARE = 'similarweb_keywords_trafficshare_export'
BQ_TABLE_PAGES_TRAFFICSHARE = 'similarweb_website_pages_traffishare'
GOOGLE_PROJECT = "Project ID"

WEBSITES = [
    {
        "domain": "bbc.com",
        "category": "news"
    },
    {
        "domain": "cnn.com",
        "category": "news"
    },
    
]

KEYWORDS = [
    {
        "keyword" : "bbc"
    },
    {
        'keyword' : 'global news'
    }
]