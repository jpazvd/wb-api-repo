import pytest
import pandas as pd
from _programs.wb_api_tools import get_data

@pytest.mark.parametrize("countries,date,expected_rows", [
    ('BRA', '2010:2012', 3),
    ('BRA', '2015:2017', 3),
])
def test_get_data_long(countries, date, expected_rows, monkeypatch):
    # Use monkeypatch to return a small CSV sample from test data
    sample_path = 'c:\\GitHub\\others\\wb-api-repo\\_tests\\test_gdp.csv'
    sample_csv = open(sample_path, 'r', encoding='utf-8').read()
    class DummyResponse:
        status_code = 200
        content = sample_csv.encode('utf-8-sig')
        @staticmethod
        def raise_for_status(): pass
    
    def dummy_get(url, params, timeout):
        return DummyResponse()

    monkeypatch.setattr('requests.Session.get', dummy_get)
    df = get_data(indicators=['NY.GDP.PCAP.PP.KD'], countries=countries, date=date, long=True)
    # Expect one row per year in date range that matches sample
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == expected_rows
    assert 'value' in df.columns
    assert 'countryiso3code' in df.columns
    assert df['countryiso3code'].iloc[0] == 'BRA'
