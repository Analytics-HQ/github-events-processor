import pandas as pd
import great_expectations as gx

from validator.data_validator import DataValidator, DataSourceType

def test_data_validator():
    validator = DataValidator('./.gx')

    config = validator.get_config()
    print("--- Configuration ---")
    print(config)
    print("---------------------")

    ds = validator.add_datasource(
        type=DataSourceType.PANDAS,
        name='pandas_datasource',
        execution_engine={
            'class_name': 'PandasExecutionEngine'
        },
        data_connectors={
            'default_runtime_data_connector_name': {
                'class_name': 'RuntimeDataConnector',
                'batch_identifiers': ['default_identifier_name']
            }
        },
    )
    print("--- Data Source ---")
    print(ds)
    print("-------------------")

    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, -5]
    })

    validator.add_dataframe_asset(asset_name="my_asset", df=df)

    validator.add_batch_definition(batch_definition_name="my_batch")

    suite = validator.add_expectation('test_suite', gx.expectations.ExpectColumnValuesToBeBetween(
        column="age", min_value=0, max_value=100
    ))
    print("--- Suite ---")
    print(suite)
    print("-------------")

    results = validator.validate(df)
    print("--- Validation Results ---")
    print(results)
    print("--------------------------")

if __name__ == "__main__":
    test_data_validator()
