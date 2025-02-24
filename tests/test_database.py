import pytest
from polars_src.database import get_table_definition

def test_valid_monitoring_layer():
    result = get_table_definition('monitoring_layer', 'monitoring_values')
    assert isinstance(result, dict)

def test_valid_landingzone_layer():
    result = get_table_definition('landingzone_layer', 'some_table_landingzone')
    assert isinstance(result, dict)

def test_valid_raw_layer():
    result = get_table_definition('raw_layer', 'some_table_raw')
    assert isinstance(result, dict)

def test_valid_datamodel_layer():
    result = get_table_definition('datamodel_layer', 'some_table_datamodel')
    assert isinstance(result, dict)

def test_valid_enriched_layer():
    result = get_table_definition('enriched_layer', 'some_table_enriched')
    assert isinstance(result, dict)

def test_invalid_layer():
    with pytest.raises(ValueError):
        get_table_definition('invalid_layer', 'some_table_datamodel')

if __name__ == '__main__':
    pytest.main()
