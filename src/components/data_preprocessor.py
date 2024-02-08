"""
summary
"""

from os import listdir
from os.path import dirname, join, normpath

import pandas as pd

from src.constants import CONFIGS
from src.exception import CustomException
from src.logger import logger
from src.utils.basic_utils import create_directories, read_yaml


class DataPreprocessor:
    """
    summary
    """

    def __init__(self):
        """
        Initializes the DataIngestion class. Reads the configuration files.
        """
        # Read the configuration files
        self.configs = read_yaml(CONFIGS).data_preprocessor

        # Input data directory
        self.interim_dir = normpath(self.configs.interim_dir)

        # Output file path
        self.processed_path = normpath(self.configs.processed_path)
        self.final_path = normpath(self.configs.final_path)

    def combine_data(self):
        """_summary_

        Raises:
            CustomException: _description_
        """
        try:
            dataframes = []

            # Iterate over the CSV files in the directory and save dataframes
            logger.info("Reading individual CSV files")
            for file in listdir(self.interim_dir):
                if file.endswith(".csv"):
                    csv_filepath = join(self.interim_dir, file)
                    df = pd.read_csv(
                        csv_filepath,
                        header=0,
                        encoding="utf-8",
                        parse_dates=["InvoiceDate"],
                    )
                    dataframes.append(df)

            # Consolidate dataframes
            logger.info("Combining all CSV files to create a consolidate dataframe")
            consolidated_df = pd.concat(dataframes, ignore_index=True, axis=0)
            return consolidated_df
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e) from e

    @staticmethod
    def clean_data(df: pd.DataFrame) -> list[pd.DataFrame]:
        """_summary_

        Args:
            df (pd.DataFrame): _description_

        Returns:
            list[pd.DataFrame]: _description_
        """
        # Dropping any null rows
        df = df.dropna(axis=0, how="any")

        # Drop cancelled transaction rows,i.e., "Invoice" value starts with "C"
        cancelled_tranx_cond = df["Invoice"].str.startswith("C")
        df = df[~cancelled_tranx_cond]

        # Split "InvoiceDate" into invoice_date and invoice_time columns
        df.loc[:, "invoice_date"] = df["InvoiceDate"].dt.date
        df.loc[:, "invoice_time"] = df["InvoiceDate"].dt.time

        # Drop "InvoiceDate" column
        df = df.drop(columns="InvoiceDate")

        # Fix datatype of columns
        df = df.astype(
            {
                "Invoice": "int32",
                "Customer ID": "int32",
                "StockCode": "string",
                "Description": "string",
                "Country": "string",
                "invoice_date": "datetime64[ns]",
            }
        )

        # Rename columns
        df = df.rename(
            columns={
                "Invoice": "invoice_no",
                "StockCode": "stock_code",
                "Description": "description",
                "Quantity": "quantity",
                "Price": "price",
                "Customer ID": "customer_id",
                "Country": "country",
            }
        )

        # Rearranged columns
        rearranged_col_list = [
            "invoice_no",
            "stock_code",
            "customer_id",
            "invoice_date",
            "invoice_time",
            "description",
            "country",
            "quantity",
            "price",
        ]

        df = df.reindex(rearranged_col_list, axis="columns")

        # Reset index on clean dataset
        clean_df = df.reset_index(drop=True)

        # Preparing dataframe for data analysis
        analysis_df = clean_df.copy(deep=True)

        # Create "sales_amount" column
        analysis_df["sales_amount"] = round(
            analysis_df["price"] * analysis_df["quantity"], 2
        )

        # Keep rows where sales_amount is greater than zero
        analysis_df = analysis_df[analysis_df["sales_amount"] > 0]

        # Keep required columns only
        analysis_cols = [
            "invoice_no",
            "customer_id",
            "invoice_date",
            "country",
            "quantity",
            "sales_amount",
        ]
        analysis_df = analysis_df[analysis_cols]

        # Reset index on analysis dataset
        analysis_df = analysis_df.reset_index(drop=True)

        return [clean_df, analysis_df]

    def save_processed_data(self):
        """_summary_

        Raises:
            CustomException: _description_
        """
        # Create directory if not exist
        create_directories([dirname(self.processed_path), dirname(self.final_path)])

        # Combine the dataframes
        combined_df = self.combine_data()
        try:
            # Preprocess the dataframes
            logger.info("Preprocessing the consolidated dataframes")
            clean_df, final_df = self.clean_data(combined_df)

            # Save dataframes as CSV files
            logger.info("Saving the clean and transformed datasets as CSV files")
            clean_df.to_csv(
                self.processed_path, index=False, encoding="utf-8", header=True
            )
            final_df.to_csv(self.final_path, index=False, encoding="utf-8", header=True)
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e) from e
