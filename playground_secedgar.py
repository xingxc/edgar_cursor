#%%

import os
import pandas as pd
import re
import requests
import calendar
import numpy as np
import logging
import utility_belt
from bs4 import BeautifulSoup
import edgar_functions
from datetime import datetime, timedelta

headers = {"User-agent": "email@email.com"}


tickers = 'jpm'

def get_submissions_for_ticker(tickers, headers, get_archive=False):

    # Get cik and recent filings
    cik = edgar_functions.cik_matching_ticker(tickers, headers=headers)
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    company_json = requests.get(url, headers=headers).json()

    additional_files = company_json['filings']['files']
    company_filing_df = pd.DataFrame(company_json["filings"]["recent"])

    # Optionally get archived filings
    if get_archive:
    
        df_archived_links = pd.DataFrame()
        df_company_filing_archived = pd.DataFrame()

        # get the links of the archived files
        for row in additional_files:
            df_archived_links = pd.concat([df_archived_links, pd.DataFrame([row])], ignore_index=True)

        # get the filings from the archived files
        for row in df_archived_links['name']:

            url_archived = f"https://data.sec.gov/submissions/{row}"
            company_json_archived = requests.get(url_archived, headers=headers).json()
            _ = pd.DataFrame(company_json_archived)
            df_company_filing_archived = pd.concat([df_company_filing_archived, _], ignore_index=True)

            print(f'Downloaded/appended: {url_archived}')

        company_filing_df = pd.concat([company_filing_df, df_company_filing_archived], ignore_index=True)

    return company_filing_df

df = get_submissions_for_ticker(tickers, headers, get_archive=True)
# %%
