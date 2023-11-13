import requests
import pandas as pd

headers = {"User-Agent": "email@address.com"}


class CursorEdgar:
    def __init__(self, header={"User-Agent": "email@address.com"}):
        self.__cik = None
        self.__df_tickers = None
        self.__headers = header
        self.__ticker = None

        self.company_facts = None
        self.company_concept = {}
        self.df_concept = {}
        self.keys_concept = None
        self._df_tickers()

    def _df_tickers(self):
        """
        query EDGAR for the DataFrame.
        """
        company_tickers = requests.get(
            "https://www.sec.gov/files/company_tickers.json", headers=self.__headers
        )

        self.__df_tickers = pd.DataFrame.from_dict(
            company_tickers.json(), orient="index"
        )
        self.__df_tickers["cik_str"] = (
            self.__df_tickers["cik_str"].astype(str).str.zfill(10)
        )
        # self.__df_tickers = self.__df_tickers.set_index(keys="cik_str")

        print(f"returned to tickers to __df_tickers")

    def _dict_query(self, dict_in, list_depth):
        for i in list_depth:
            dict_in = dict_in[i]
        return dict_in

    def get_df_tickers(self):
        """
        return the ticker CIK DataFrame.
        """
        return self.__df_tickers

    def get_cik(self, ticker):
        """
        Input ticker symbol and return the CIK for the ticker
        """
        self.__cik = self.__df_tickers[self.__df_tickers["ticker"] == ticker][
            "cik_str"
        ].iloc[0]
        # self.__cik = self.__df_tickers[self.__df_tickers["ticker"] == ticker].index[0]
        return self.__cik

    def get_keys_concept(self, style="us-gaap"):
        """
        return the keys for company concept query
        """

        return list(self.company_facts.json()["facts"][style].keys())

    def query_company_facts(self):
        """
        Query for company facts
        """

        if self.__cik is None:
            raise Exception("cik is None: run cik_return method")

        self.company_facts = requests.get(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{self.__cik}.json",
            headers=self.__headers,
        )

    def query_get_df_concept(self, key_concept, list_depth=["units", "USD"]):
        """
        1. execute the request query
        2. execute the get dataframe
        """
        self.query_company_concept(key_concept)
        self.get_df_concept(key_concept, list_depth=list_depth)
        return self.df_concept[key_concept]

    def query_company_concept(self, key_concept):
        """
        Query request for company concepts
        """
        if self.__cik is None:
            raise Exception("cik is None: run cik_return method")

        self.company_concept[key_concept] = requests.get(
            (
                f"https://data.sec.gov/api/xbrl/companyconcept/CIK{self.__cik}"
                f"/us-gaap/{key_concept}.json"
            ),
            headers=self.__headers,
        )

    def get_df_concept(self, key_concept, list_depth=["units", "USD"]):
        """
        Returns the concept dataframe from the concept query
        """
        _ = self._dict_query(self.company_concept[key_concept].json(), list_depth)
        self.df_concept[key_concept] = pd.DataFrame.from_dict(_)

        return self.df_concept[key_concept]
