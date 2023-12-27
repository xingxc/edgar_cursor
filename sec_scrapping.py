# %%
import requests
import pandas as pd


headers = {"User-agent": "email@email.com"}


def cik_matching_ticker(ticker, headers=headers):
    ticker = ticker.upper().replace(".", "-")
    ticker_json = requests.get(
        "https://www.sec.gov/files/company_tickers.json", headers=headers
    ).json()

    for company in ticker_json.values():
        if company["ticker"] == ticker:
            cik = str(company["cik_str"]).zfill(10)
            return cik
    raise ValueError(f"Ticker {ticker} not found in SEC CIK list")


def get_submissions_for_ticker(tickers, headers=headers, only_filings_df=False):
    cik = cik_matching_ticker(tickers)
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    company_json = requests.get(url, headers=headers).json()
    if only_filings_df:
        return pd.DataFrame(company_json["filings"]["recent"])
    return company_json


def get_filter_filing(ticker, ten_k=True, accession_number_only=False, headers=headers):
    company_filing_df = get_submissions_for_ticker(
        ticker, headers=headers, only_filings_df=True
    )
    if ten_k:
        df = company_filing_df[company_filing_df["form"] == "10-K"]
    else:
        df = company_filing_df[company_filing_df["form"] == "10-Q"]

    if accession_number_only:
        df = df.set_index("reportDate")
        return df["accessionNumber"]
    else:
        return df

#%%
ticker = "pypl"
data_submissions = get_submissions_for_ticker(ticker, headers=headers, only_filings_df=False)


#%%
df = get_filter_filing(
    ticker, ten_k=False, accession_number_only=False, headers=headers
)

# %%
