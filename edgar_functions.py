import pandas as pd
import requests
import calendar
import numpy as np
import logging
from bs4 import BeautifulSoup

statement_keys_map = {
    "balance_sheet": [
        "balance sheet",
        "balance sheets",
        "statement of financial position",
        "consolidated balance sheets",
        "consolidated balance sheet",
        "consolidated financial position",
        "consolidated balance sheets - southern",
        "consolidated statements of financial position",
        "consolidated statement of financial position",
        "consolidated statements of financial condition",
        "combined and consolidated balance sheet",
        "condensed consolidated balance sheets",
        "consolidated balance sheets, as of december 31",
        "dow consolidated balance sheets",
        "consolidated balance sheets (unaudited)",
    ],
    "income_statement": [
        "income statement",
        "income statements",
        "statement of earnings (loss)",
        "statements of consolidated income",
        "consolidated statements of operations",
        "consolidated statement of operations",
        "consolidated statements of earnings",
        "consolidated statement of earnings",
        "consolidated statements of income",
        "consolidated statement of income",
        "consolidated income statements",
        "consolidated income statement",
        "condensed consolidated statements of earnings",
        "consolidated results of operations",
        "consolidated statements of income (loss)",
        "consolidated statements of income - southern",
        "consolidated statements of operations and comprehensive income",
        "consolidated statements of comprehensive income",
    ],
    "cash_flow_statement": [
        "cash flows statement",
        "cash flows statements",
        "statement of cash flows",
        "statements of consolidated cash flows",
        "consolidated statements of cash flows",
        "consolidated statement of cash flows",
        "consolidated statement of cash flow",
        "consolidated cash flows statements",
        "consolidated cash flow statements",
        "condensed consolidated statements of cash flows",
        "consolidated statements of cash flows (unaudited)",
        "consolidated statements of cash flows - southern",
    ],
}


links_logged = {}


def cik_matching_ticker(ticker, headers):
    """
    Input:
        ticker [str]: ticker symbol
        headers [dict]: headers for the requests.get() function

    Returns:
        cik [str]: cik number for the ticker

    Description:
        - Gets the cik to ticker json from the SEC website
        - Loops through the json to find the cik for the specific ticker input
        - Returns the cik number for the ticker input
    """
    ticker = ticker.upper().replace(".", "-")
    ticker_json = requests.get(
        "https://www.sec.gov/files/company_tickers.json", headers=headers
    ).json()

    for company in ticker_json.values():
        if company["ticker"] == ticker:
            cik = str(company["cik_str"]).zfill(10)
            return cik
    raise ValueError(f"Ticker {ticker} not found in SEC CIK list")


def get_submissions_for_ticker(tickers, headers, only_filings_df=False):
    """
    Input:
        ticker [str]: ticker symbol
        headers [dict]: headers for the requests.get() function
        only_filings_df [bool]: if True, returns a dataframe of the filings for the ticker

    Returns:
        if only_filings_df is True:
            return filings_dataframe [pd.DataFrame]: dataframe of the filings for the ticker
        if only_filings_df is False:
            return company_json [dict]: dictionary of the filings for the ticker

    """
    cik = cik_matching_ticker(tickers, headers=headers)
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    company_json = requests.get(url, headers=headers).json()
    if only_filings_df:
        return pd.DataFrame(company_json["filings"]["recent"])
    return company_json


def get_filter_filing(ticker, headers, ten_k=True, accession_number_only=False):
    """
    - if accession_number_only is false: returns a dataframe of all the
      10-K or 10-Q filings for a given ticker.

    - if accession_number_only is true: returns a series of
      the accession numbers for all the 10-K or 10-Q filings for a given ticker.
    """

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


def get_facts_json(ticker, headers):
    """
    returns a json of all the facts for a given ticker.
    """

    cik = cik_matching_ticker(ticker)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    company_facts = requests.get(url, headers=headers).json()
    return company_facts


def facts_to_df(ticker, headers):
    """
    returns a dataframe of all the facts for a given ticker.
    """

    company_facts = get_facts_json(ticker, headers=headers)
    us_gaap_data = company_facts["facts"]["us-gaap"]
    df_data = []

    for fact, details in us_gaap_data.items():
        for unit in details["units"].keys():
            for item in details["units"][unit]:
                row = item.copy()
                row["fact"] = fact
                df_data.append(row)

    df = pd.DataFrame(df_data)
    df["end"] = pd.to_datetime(df["end"])
    df["start"] = pd.to_datetime(df["start"])
    df = df.drop_duplicates(subset=["fact", "end", "val"])
    df.set_index("end", inplace=True)
    labels_dict = {fact: details["label"] for fact, details in us_gaap_data.items()}

    return df, labels_dict


def annual_facts(ticker, headers):
    """
    returns a dataframe of all the annual facts for a given ticker.
    """

    accession_nums = get_filter_filing(
        ticker, ten_k=True, accession_number_only=True, headers=headers
    )
    df, labels = facts_to_df(ticker, headers=headers)
    ten_k = df[df["accn"].isin(accession_nums)]
    ten_k = ten_k[ten_k.index.isin(accession_nums.index)]
    pivot = ten_k.pivot_table(values="val", index="end", columns="fact")
    pivot.rename(columns=labels, inplace=True)
    return pivot.T


def quarterly_facts(ticker, headers):
    """
    returns a dataframe of all the quarterly facts for a given ticker.
    """

    accession_nums = get_filter_filing(
        ticker, ten_k=False, accession_number_only=True, headers=headers
    )
    df, labels = facts_to_df(ticker, headers=headers)
    ten_q = df[df["accn"].isin(accession_nums)]
    ten_q = ten_q[ten_q.index.isin(accession_nums.index)]
    ten_q = ten_q[ten_q.index.isin(accession_nums.index)].reset_index(drop=False)
    ten_q = ten_q.drop_duplicates(subset=["fact", "end"], keep="last")
    pivot = ten_q.pivot_table(values="val", index="end", columns="fact")
    pivot.rename(columns=labels, inplace=True)
    return pivot.T


def get_file_name(report):
    """
    Looks for the HtmlFileName tag returns the text if it exists, otherwise
    looks for the XmlFileName tag returns the text if it exists, otherwise
    returns None.
    """
    html_file_name = report.find("HtmlFileName")
    xml_file_name = report.find("XmlFileName")

    if html_file_name:
        return html_file_name.text
    elif xml_file_name:
        return xml_file_name.text
    else:
        return None


def is_file_statement(long_name, short_name, file_name):
    """
    Returns boolean [True] if (long_name, short_name, file_name is not None),
    and "Statement" is in the long_name.
    """
    return (
        long_name is not None
        and short_name is not None
        and file_name is not None
        and "Statement" in long_name.text
    )


def get_statement_file_names_in_filling_summary(ticker, acc_num, headers):
    """
    Inputs:
        ticker [str]: ticker symbol
        acc_num [str]: accession number
        headers [dict]: headers for the requests.get() function

    Returns:
        statement_file_names_dict [dict]: dictionary of statement names and file names

    Description:
        - Gets the cik number from the ticker symbol
        - Gets the filing summary xml from baselink and return the XML as string
        - Parses the filing summary XML string into a BeautifulSoup object
        - Loops through the BeautifulSoup object to find the file names of the statements
            - calls is_file_statement() to check if the report is a statement
        - Returns statement_file_names_dict which is a dictionary of statement names and file names
    """
    try:
        session = requests.Session()
        cik = cik_matching_ticker(ticker, headers=headers)
        baselink = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_num}/"
        filing_summary_link = f"{baselink}/FilingSummary.xml"
        filing_summary_response = session.get(
            filing_summary_link, headers=headers
        ).content.decode("utf-8")

        filing_summary_soup = BeautifulSoup(filing_summary_response, "lxml-xml")
        statement_file_name_dict = {}

        for report in filing_summary_soup.find_all("Report"):
            file_name = get_file_name(report)
            short_name, long_name = report.find("ShortName"), report.find("LongName")
            # print(f'short_name: {short_name} ; long_name: {long_name} ; file_name: {file_name}')
            if is_file_statement(long_name, short_name, file_name):
                statement_file_name_dict[short_name.text.lower()] = file_name

        return statement_file_name_dict

    except requests.RequestException as e:
        print(f"An error occured: {e}")
        return {}


def get_statement_soup(ticker, acc_num, statement_name, headers, statement_keys_map):
    """
    Args:
        - ticker [str]: ticker symbol
        - acc_num [str]: accession number
        - statement_name [str]: name of the statement, e.g. "balance_sheet"
        - headers [dict]: headers for the requests.get() function
        - statement_keys_map [dict]: dictionary of statement names and possible keys

    Returns:
        - BeautifulSoup object of the html or xml of the statement from baselink.
        - Baselink: https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}/{statement_ID}

    Description:
        - Gets the cik number from the ticker symbol
        - Gets the filing summary xml from baselink, cik, and acc_num and return the xml as dict
        - Loops through the possible statement keys to find the file name of the statement
        - Create the statement link from the base link and file name
        - Query the statement link and return the BeautifulSoup object
        - returns the BeautifulSoup object of the html or xml of the statement
    """

    session = requests.Session()
    cik = cik_matching_ticker(ticker, headers=headers)
    base_link = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_num}"

    statement_file_name_dict = get_statement_file_names_in_filling_summary(
        ticker, acc_num, headers
    )

    statement_link = None

    for possible_key in statement_keys_map.get(statement_name.lower(), []):
        file_name = statement_file_name_dict.get(possible_key.lower())
        if file_name:
            statement_link = f"{base_link}/{file_name}"
            break

    if not statement_link:
        raise ValueError(f"Could not find statement file name for {statement_name}")

    try:
        statement_response = session.get(statement_link, headers=headers)
        statement_response.raise_for_status()  # Check if the request was successful
        links_logged[
            f"{ticker}-{statement_name}-{file_name}-{acc_num}"
        ] = statement_link

        if statement_link.endswith(".xml"):
            return BeautifulSoup(
                statement_response.content, "lxml-xml", from_encoding="utf-8"
            )
        else:
            return BeautifulSoup(statement_response.content, "lxml")

    except requests.RequestException as e:
        raise ValueError(f"Error fetching the statement: {e}")


def extract_columns_values_and_dates_from_statement(soup):
    """
    Args:
        soup (BeautifulSoup): BeautifulSoup object of the statement.

    Returns:
        list: A list of statement column names.
        list: A list of statement values.
        pd.DatetimeIndex: A Pandas DatetimeIndex object containing the extracted dates.

    Description:
        - call get_datetime_index_dates_from_statement to get the date_time_index
        - find all tables in soup and iterate over them
        - check table header (th) for unit multiplier and special case scenario
            - special case scenario: check for values like EPS
        - search each row of table (tr) for "onclick" attribute
            - if "onclick" is not found, skip the row
            - if "onclick" is found
                - append column title to columns list from "onclick" attribute
                - for each cell in the row (td)
                    - find all elements with class "text", "nump", or "num"
                        - nump: positive values
                        - num: negative values
                        - text: skip
        - return columns, values, and date_time_index
    """

    columns = []
    values_set = []

    date_time_index = get_datetime_index_dates_from_statement(soup=soup)

    for table in soup.find_all("table"):
        unit_multiplier = 1
        special_case = False

        # Check table header for unit multiplier
        table_header = table.find("th")
        if table_header:
            header_text = table_header.get_text()

            if "in Thousands" in header_text:
                unit_multiplier = 1

            elif "in Millions" in header_text:
                unit_multiplier = 1000

            # Check for special case scenario
            if "unless otherwise specified" in header_text:
                special_case = True

        # Process each row of the table
        for i, row in enumerate(table.select("tr")):
            onclick_elements = row.select("td.pl a, td.pl.custom a")
            if not onclick_elements:
                continue  # Skip rows without onclick elements

            # Extract column title from "onclick" attribute
            onclick_attr = onclick_elements[0]["onclick"]
            column_title = onclick_attr.split("defref_")[-1].split("',")[0]
            columns.append(column_title)

            # Inititate values array with NaNs
            values = [np.NaN] * len(date_time_index)

            # Process each cell in the row
            for i, cell in enumerate(row.select("td.text, td.nump, td.num")):
                if "text" in cell.get("class"):
                    continue  # Skip text cells

                value = (
                    cell.text.replace("$", "")
                    .replace(",", "")
                    .replace("(", "")
                    .replace(")", "")
                    .strip()
                )

                # ------ Keep numbers and decimals only in string function ------
                num = "1234567890."

                def filtering_func(x):
                    return x in num

                # Filter creates iterator and list converts it back to list.
                allowed = list(filter(filtering_func, value))
                value = "".join(allowed)
                # ------ Keep numbers and decimals only in string function ------

                if value:
                    value = float(value)

                    if special_case:
                        value = value / 1000

                    else:
                        if "nump" in cell.get("class"):
                            values[i] = (
                                value * unit_multiplier
                            )  # converting from unit to thousands
                        else:
                            values[i] = -value * unit_multiplier

            values_set.append(values)
    return columns, values_set, date_time_index


def get_datetime_index_dates_from_statement(soup: BeautifulSoup) -> pd.DatetimeIndex:
    """
    Args:
        soup (BeautifulSoup): BeautifulSoup object of the statement.

    Returns:
        pd.DatetimeIndex: A Pandas DatetimeIndex object containing the extracted dates.

    Description:
        - find all table headers (th) with class "th"
        - extract the date from the div tag and create list of strings
        - standardize the date strings
        - convert the list of strings to a Pandas DatetimeIndex object
        - return the DatetimeIndex object
    """
    table_headers = soup.find_all("th", {"class": "th"})
    dates = [str(th.div.string) for th in table_headers if th.div and th.div.string]
    dates = [standardize_date(date).replace(".", "") for date in dates]
    date_time_index = pd.to_datetime(dates)

    return date_time_index


def standardize_date(date: str) -> str:
    """
    Standardizes date strings by replacing abbreviations with full month names.

    Args:
        date (str): The date string to be standardized.

    Returns:
        str: The standardized date string.

    Description:
        - Loops through the abbreviations and and replaces them with the full names
        - Returns the standardized date string
    """
    for abbr, full in zip(calendar.month_abbr[1:], calendar.month_name[1:]):
        date = date.replace(abbr, full)
    return date


def keep_numbers_and_decimals_only_in_string(mixed_string: str):
    """
    Filters a string to keep only numbers and decimal points.

    Args:
        mixed_string (str): The string containing mixed characters.

    Returns:
        str: String containing only numbers and decimal points.
    """
    num = "1234567890."
    allowed = list(filter(lambda x: x in num, mixed_string))
    return "".join(allowed)


def create_dataframe_of_statement_values_columns_dates(
    values_set, columns, index_dates
) -> pd.DataFrame:
    """
    Args:
        values_set (list): List of values for each column.
        columns (list): List of column names.
        index_dates (pd.DatetimeIndex): DatetimeIndex for the DataFrame index.

    Returns:
        pd.DataFrame: DataFrame constructed from the given data.

    Description:
        - Create dataframe from values_set, columns, and index_dates
        - return dataframe
    """
    transposed_values_set = list(zip(*values_set))
    df = pd.DataFrame(transposed_values_set, columns=columns, index=index_dates)
    return df


def process_one_statement(ticker, acc_num, statement_name, headers):
    """
    Args:
        ticker (str): Ticker of the company.
        acc_num (str): Accession number of the filing.
        statement_name (str): Name of the statement.
        headers (dict): Headers for the request.

    Returns:
        pd.DataFrame: DataFrame containing the statement data.


    Description:
        - Get the BeautifulSoup object of the statement from get_statement_soup
        - Extract the columns, values, and date_time_index from extract_columns_values_and_dates_from_statement
        - Create a DataFrame from create_dataframe_of_statement_values_columns_dates
        - Transpose the DataFrame and drop duplicates
        - Return the DataFrame

    """

    try:
        soup = get_statement_soup(
            ticker, acc_num, statement_name, headers, statement_keys_map
        )

    except Exception as e:
        logging.error(
            f"Failed to get statement soup: {e} for accession number: {acc_num}"
        )
        return None

    if soup:
        try:
            (
                columns,
                values_set,
                date_time_index,
            ) = extract_columns_values_and_dates_from_statement(soup=soup)

            df = create_dataframe_of_statement_values_columns_dates(
                values_set, columns, date_time_index
            )

            if not df.empty:
                df = df.T.drop_duplicates()
            else:
                logging.warning(f"Empty DataFrame for accession number: {acc_num}")
                return None

            return df
        except Exception as e:
            logging.error(f"Error processing soup to DataFrame: {e}")
            return None
