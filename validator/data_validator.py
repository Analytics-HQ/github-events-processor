import logging
from enum import Enum

import pandas as pd

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource
from great_expectations.exceptions.exceptions import DataContextError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class DataSourceType(Enum):
    PANDAS = "PANDAS"

class DataValidator:
    def __init__(self, project_root_dir: str = None):
        self.ds = None
        self.suite = None
        self.asset = None

        if project_root_dir:
            self.context = gx.get_context(mode="file", project_root_dir=project_root_dir)
            logging.info(f"File Data Context initialized from: {project_root_dir}")
        else:
            self.context = gx.get_context(mode="file")
            logging.info("File Data Context initialized from current directory.")

    def get_config(self):
        return self.context.get_config()

    def add_datasource(self, type: DataSourceType, name: str, execution_engine: dict, data_connectors: dict):
        try:
            self.ds = self.context.data_sources.get(name)
            logging.info(f"Datasource '{name}' already exists; updating it.")
        except KeyError:
            if type == DataSourceType.PANDAS:
                self.ds = self.context.data_sources.add_pandas(name=name)
                logging.info(f"Datasource '{name}' created.")
            else:
                raise NotImplementedError(f"Datasource type {type} not implemented.")

        return self.ds

    def add_dataframe_asset(self, asset_name: str, df: pd.DataFrame):
        if not isinstance(self.ds, PandasDatasource):
            raise TypeError("Datasource is not a PandasDatasource. Make sure to call add_datasource() with type PANDAS.")

        self.asset = self.ds.add_dataframe_asset(name=asset_name)

        logging.info(f"DataFrame asset '{asset_name}' added to datasource '{self.ds.name}'.")

    def add_batch_definition(self, batch_definition_name: str):
        if self.asset is None:
            raise ValueError("No data asset has been added. Call add_dataframe_asset() first.")
        
        self.batch_definition = self.asset.add_batch_definition_whole_dataframe(batch_definition_name)
        logging.info(f"Batch definition '{batch_definition_name}' added to asset '{self.asset.name}'.")

    def add_expectation(self, suite_name: str, expectation) -> ExpectationSuite:
        try:
            self.suite = self.context.suites.get(name=suite_name)
            logging.info(f"Expectation suite '{self.suite.name}' loaded.")
        except Exception:
            self.suite = self.context.suites.add(ExpectationSuite(name=suite_name))
            logging.info(f"Expectation suite '{suite_name}' created.")

        self.suite.add_expectation(expectation)

        logging.info(f"Expectation added to suite '{self.suite.name}'.")
        return self.suite

    def validate(self, df: pd.DataFrame) -> dict:
        if self.asset is None:
            raise ValueError("No data asset has been added. Call add_dataframe_asset() first.")
        if self.suite is None:
            raise ValueError("No expectation suite has been created. Call add_expectation() first.")

        validator = self.context.get_validator(
            datasource_name=self.ds.name,
            data_connector_name="default_runtime_data_connector_name",  # required
            data_asset_name=self.asset.name,
            batch_data={"dataframe": df},
            batch_identifiers={"default_identifier_name": "runtime_batch_001"},
            expectation_suite=self.suite
        )

        return validator.validate()
