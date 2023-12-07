from config.config import Config
from utils.utils import Utils
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import json
import os


class GetReports:
    def __init__(self):
        """
        initialize the packages from Config and Utils
        """
        self.config = Config()
        self.utils = Utils()

    def NADAC_mapping(self):
        """
        get the weekly NADAC csv file url and read via pd
        join the local orange_book_nadac_mapper file to map
        with the cost and product number with each NDC
        """
        response = self.utils.make_request(
            method="GET", url=self.config.NADAC["url"], payloads=None
        )
        if response.ok:
            data = json.loads(response.content)
        for format in self.utils.recusrively_search_item("format", data):
            if format == "csv":
                downloadURL_list = self.utils.recusrively_search_item(
                    "downloadURL", data
                )
                for url in downloadURL_list:
                    df = pd.read_csv(url)
        df["As of Date"] = pd.to_datetime(df["As of Date"])
        df = df[(df["As of Date"] >= "2023-01-01") & (df["As of Date"] < "2024-01-01")]
        df_weekly = (
            df.groupby(["NDC", "As of Date"])
            .agg({"NADAC_Per_Unit": "mean"})
            .reset_index()
        )
        df_mapper = pd.read_csv(
            os.path.join(os.getcwd(), "orange_book_nadac_mapper.csv")
        )
        df_mapper = (
            df_mapper.groupby(["nadac_NDC", "nda_number"])
            .agg({"product_number": "sum"})
            .reset_index()
        )
        df_merge = pd.merge(
            df_weekly, df_mapper, how="inner", left_on="NDC", right_on="nadac_NDC"
        )
        df_merge.drop(columns=["nadac_NDC"], inplace=True)
        df_merge.sort_values(by=["NDC", "As of Date"], inplace=True)
        return df_merge

    def orange_book_retrieive(self):
        """
        retrieve the monthly updated product.txt in a zipped file
        filter out only approve date between 2013 and 2023 for nasal
        spray only
        """

        repsonse = self.utils.make_request(
            method="GET", url=self.config.orange_book["zip_url"], payloads=None
        )
        if repsonse.ok:
            with ZipFile(BytesIO(repsonse.content), "r") as zip_file:
                with zip_file.open(
                    self.config.orange_book["zip_file_name"]
                ) as extracted_file:
                    df = pd.read_csv(extracted_file, sep="~")
        if not df.empty:
            df = df[df["Approval_Date"] != "Approved Prior to Jan 1, 1982"]
            df["Approval_Date"] = pd.to_datetime(df["Approval_Date"])
            df_filtered = df[
                (df["Approval_Date"] >= "2013")
                & (df["Approval_Date"] <= "2023")
                & (df["DF;Route"].str.contains("NASAL"))
                & (df["DF;Route"].str.contains("SPRAY"))
            ]
        return df_filtered

    def parse_the_FEARS(self, df_orange_book):
        """parse the side effect data from FEARS database via api
        in order to find out number of side effect corresonding to
        each drug and term
        """
        res_dict = {}
        for product_name in df_orange_book["Trade_Name"]:
            payloads = {
                "search": f"{product_name}",
                "count": self.config.FEARS["count"],
            }
            response = self.utils.make_request(
                method="GET", url=self.config.FEARS["url"], payloads=payloads
            )
            # each product name assciate with each a list of result
            res_dict[f"{product_name}"] = response.json()["results"]
            # make the key unique to parse the reuslt
        df_FEARS = pd.DataFrame.from_dict(
            {
                (i, j): res_dict[i][j]
                for i in res_dict.keys()
                for j in range(len(res_dict[i]))
            },
            orient="index",
        )
        df_FEARS.reset_index(inplace=True)
        df_FEARS.rename(columns={"level_0": "Drug", "level_1": "Index"}, inplace=True)
        df_FEARS.drop(columns=["Index"], inplace=True)

        return df_FEARS


def main():
    # Create an instance of GetReports
    get_reports_instance = GetReports()

    df_merged = get_reports_instance.NADAC_mapping()
    df_filtered_orange_book = get_reports_instance.orange_book_retrieive()
    df_fears = get_reports_instance.parse_the_FEARS(df_filtered_orange_book)

    print(df_merged.head())
    print(df_filtered_orange_book.head())
    print(df_fears.head())


if __name__ == "__main__":
    main()
