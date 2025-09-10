import sys, os
# Add repo root to path for recognizing _programs package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sys, os
# Add repo root to path for recognizing _programs package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
import requests
import pandas as pd
from _programs.wb_api_tools import get_data

@pytest.mark.parametrize("countries,date,expected_rows", [
    ('BRA', '2010:2012', 3),
    ('BRA', '2015:2017', 3),
])
def test_get_data_long(countries, date, expected_rows, monkeypatch):
    # Use monkeypatch to return a small wide-format CSV sample inline
    sample_csv = (
        'Country Name,Country Code,Indicator Name,Indicator Code,2010,2011,2012\n'
        'Brazil,BRA,GDP per capita,NY.GDP.PCAP.PP.KD,18062.158110,18627.810453,18832.219553\n'
    )
    class DummyResponse:
        status_code = 200
        content = sample_csv.encode('utf-8')
        def raise_for_status(self):
            pass
    
    def dummy_get(*args, **kwargs):
        return DummyResponse()

    # Override Session.get to return dummy CSV
    monkeypatch.setattr(requests.Session, 'get', dummy_get)
    df = get_data(indicators=['NY.GDP.PCAP.PP.KD'], countries=countries, date=date, long=True)
    # Expect one row per year in date range that matches sample
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == expected_rows
    assert 'value' in df.columns
    assert 'countryiso3code' in df.columns
    assert df['countryiso3code'].iloc[0] == 'BRA'
