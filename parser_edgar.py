import numpy as np
import pandas as pd



def accumulate_data_parsing(df):
    '''
    USE: for accumulated data; revenue, expense, profit etc.
    DO NOT USE: for fixed data; total assets, cash on hand etc.
    '''

    # Tesla
    quarters_dict = {
        "FY": [r"\d\d\d\d-01-01", r"\d\d\d\d-12-31"],
        "Q1": [r"\d\d\d\d-01-01", r"\d\d\d\d-03-31"],
        "Q2": [r"\d\d\d\d-04-01", r"\d\d\d\d-06-30"],
        "Q3": [r"\d\d\d\d-07-01", r"\d\d\d\d-09-30"],
    }

    # Apple
    quarters_dict = {
        "FY": [r"\d\d\d\d-01-01", r"\d\d\d\d-12-31"],
        "Q1": [r"\d\d\d\d-01-01", r"\d\d\d\d-03-31"],
        "Q2": [r"\d\d\d\d-04-01", r"\d\d\d\d-06-30"],
        "Q3": [r"\d\d\d\d-07-01", r"\d\d\d\d-09-30"],
    }

    for k, v in quarters_dict.items():
        _ = df[df["start"].str.contains(v[0])]
        _ = _[_["end"].str.contains(v[1])]
        _ = _.drop_duplicates(subset=["start", "end"], keep="last")

        _["start"] = pd.to_datetime(_["start"], format=r"%Y-%m-%d")
        _["end"] = pd.to_datetime(_["end"], format=r"%Y-%m-%d")

        _ = _.reset_index()

        quarters_dict[k] = _

    # %  Backout Q4 performance from full year

    years_Q1 = set(pd.DatetimeIndex(quarters_dict["Q1"]["start"]).year)
    years_Q2 = set(pd.DatetimeIndex(quarters_dict["Q2"]["start"]).year)
    years_Q3 = set(pd.DatetimeIndex(quarters_dict["Q3"]["start"]).year)
    years_FY = set(pd.DatetimeIndex(quarters_dict["FY"]["start"]).year)
    years_intersection = sorted(list(years_Q1 & years_Q2 & years_Q3 & years_FY))


    quarters_dict["Q4"] = pd.DataFrame(
        [],
        columns=quarters_dict["Q1"].columns,
        index=np.arange(0, years_intersection.__len__(), 1),
    )

    for i, year in enumerate(years_intersection):
        mask_q1 = quarters_dict["Q1"]["start"].dt.year == year
        mask_q2 = quarters_dict["Q2"]["start"].dt.year == year
        mask_q3 = quarters_dict["Q3"]["start"].dt.year == year
        mask_FY = quarters_dict["FY"]["start"].dt.year == year

        quarters_dict["Q4"]["start"].loc[i] = f"{year}-10-01"
        quarters_dict["Q4"]["end"].loc[i] = f"{year}-12-31"

        quarters_dict["Q4"]["val"].loc[i] = (
            quarters_dict["FY"][mask_FY]["val"].values
            - (
                quarters_dict["Q1"][mask_q1]["val"].values
                + quarters_dict["Q2"][mask_q2]["val"].values
                + quarters_dict["Q3"][mask_q3]["val"].values
            )
        )[0]

    quarters_dict["Q4"]["start"] = pd.to_datetime(
        quarters_dict["Q4"]["start"], format=r"%Y-%m-%d"
    )
    quarters_dict["Q4"]["end"] = pd.to_datetime(
        quarters_dict["Q4"]["end"], format=r"%Y-%m-%d"
    )

    df_history = pd.DataFrame([], columns=["start", "end", "val"])
    for i, (k, v) in enumerate(quarters_dict.items()):
        if k == "FY":
            pass
        else:
            df_history = pd.concat([df_history, v[["start", "end", "val"]]])

    df_history = df_history.sort_values("start")

    return quarters_dict, df_history