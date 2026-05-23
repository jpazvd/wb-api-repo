import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'py')))
import pytest
import requests
import pandas as pd
from wb_api_tools import get_data

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
    df = get_data(indicators=['NY.GDP.PCAP.PP.KD'], countries=countries, date=date,
                  long=True, no_basic=True)  # skip country-context merge for this test
    # Expect one row per year in date range that matches sample
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == expected_rows
    assert 'value' in df.columns
    assert 'countryiso3code' in df.columns
    assert df['countryiso3code'].iloc[0] == 'BRA'


# --- Phase-5 country-context auto-merge in get_data() (PR B C5) -------

@pytest.fixture
def _stub_csv_session(monkeypatch):
    """Stub requests.Session.get with a tiny CSV the parser can handle.
    Single-indicator, 3 countries (BRA, USA, ROW), 1 year."""
    sample = (
        'Country Name,Country Code,Indicator Name,Indicator Code,2020\n'
        'Brazil,BRA,GDP per capita,NY.GDP.PCAP.PP.KD,14764.0\n'
        'United States,USA,GDP per capita,NY.GDP.PCAP.PP.KD,63206.0\n'
        'Rotopia,ROW,GDP per capita,NY.GDP.PCAP.PP.KD,9999.0\n'
    )

    class DummyResponse:
        status_code = 200
        content = sample.encode('utf-8')
        def raise_for_status(self): pass

    monkeypatch.setattr(requests.Session, 'get', lambda *a, **kw: DummyResponse())


@pytest.fixture
def _stub_basic_context(monkeypatch):
    """Stub _get_basic_context so the auto-merge happens with a known
    2-country lookup (BRA, USA — ROW intentionally absent so we can
    verify left-join behaviour)."""
    import wb_api_tools as t
    fake_bc = pd.DataFrame([
        {'countryiso3code': 'BRA', 'region': 'LCN', 'regionname': 'Latin America',
         'adminregion': 'LAC', 'adminregionname': 'L.America (developing)',
         'incomelevel': 'UMC', 'incomelevelname': 'Upper middle income',
         'lendingtype': 'IBD', 'lendingtypename': 'IBRD'},
        {'countryiso3code': 'USA', 'region': 'NAC', 'regionname': 'North America',
         'adminregion': '',    'adminregionname': '',
         'incomelevel': 'HIC', 'incomelevelname': 'High income',
         'lendingtype': 'LNX', 'lendingtypename': 'Not classified'},
    ])
    t._BASIC_CONTEXT_CACHE = None  # force re-load
    monkeypatch.setattr(t, '_get_basic_context', lambda: fake_bc)


def test_get_data_default_auto_merges_country_context(_stub_csv_session, _stub_basic_context):
    """Default no_basic=False -> 8 context columns appended."""
    df = get_data(indicators=['NY.GDP.PCAP.PP.KD'], countries='all', date='2020', long=True)
    bc_cols = {'region', 'regionname', 'adminregion', 'adminregionname',
               'incomelevel', 'incomelevelname', 'lendingtype', 'lendingtypename'}
    assert bc_cols.issubset(set(df.columns)), f'missing context cols: {bc_cols - set(df.columns)}'
    # Verify per-row values: BRA gets LCN, USA gets NAC, ROW gets NaN (left join)
    bra = df[df['countryiso3code'] == 'BRA'].iloc[0]
    assert bra['region'] == 'LCN' and bra['incomelevelname'] == 'Upper middle income'
    usa = df[df['countryiso3code'] == 'USA'].iloc[0]
    assert usa['region'] == 'NAC' and usa['incomelevelname'] == 'High income'
    row = df[df['countryiso3code'] == 'ROW'].iloc[0]
    assert pd.isna(row['region']), 'ROW not in fake_bc -> left-join NaN expected'


def test_get_data_no_basic_skips_merge(_stub_csv_session, _stub_basic_context):
    """no_basic=True -> no context columns; _get_basic_context not invoked."""
    df = get_data(indicators=['NY.GDP.PCAP.PP.KD'], countries='all', date='2020',
                  long=True, no_basic=True)
    bc_cols = {'region', 'regionname', 'adminregion', 'adminregionname',
               'incomelevel', 'incomelevelname', 'lendingtype', 'lendingtypename'}
    leaked = bc_cols & set(df.columns)
    assert not leaked, f'no_basic=True should suppress context cols, but got: {leaked}'
    # Still has the data columns
    assert {'countryiso3code', 'country', 'date', 'value'}.issubset(set(df.columns))
