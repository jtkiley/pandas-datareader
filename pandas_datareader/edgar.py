import re

from pandas import read_csv
from pandas.io.common import ZipFile
from pandas.compat import StringIO
from pandas.compat import BytesIO

from pandas_datareader.base import _BaseReader


_URL_FULL = 'ftp://ftp.sec.gov/edgar/full-index/master.zip'
_URL_DAILY = ('ftp://ftp.sec.gov/edgar/data/'
              '1000032/0001209191-15-082911.txt')
_COLUMNS = ['cik', 'company_name', 'form_type', 'date_filed', 'filename']
_DIVIDER = re.compile('--------------')
_EDGAR = 'edgar/'
_EDGAR_RE = re.compile(_EDGAR)


class EdgarIndexReader(_BaseReader):
    """
    Get master index from the SEC's EDGAR database.

    Returns
    -------
    edgar_index : pandas.DataFrame.
        DataFrame of EDGAR index.
    """

    @property
    def url(self):
        if self.symbols == 'full':
            return _URL_FULL
        elif self.symbols == 'daily':
            return _URL_DAILY
        else:
            return _URL_FULL  # Should probably raise or use full unless daily.

    def _read_zipfile(self, url):

        zipf = BytesIO(self._get_response(url).content)

        with ZipFile(zipf, 'r') as zf:
            data = zf.open(zf.namelist()[0]).read().decode()

        return StringIO(data)

    def _read_one_data(self, url, params):

        index_file = self._read_zipfile(url)
        index_file = self._remove_header(index_file)

        index = read_csv(index_file, delimiter='|', header=None,
                         index_col=False, names=_COLUMNS,
                         low_memory=False)
        index['filename'] = index['filename'].map(self._fix_old_file_paths)
        return index

    def _remove_header(self, data):
        header = True
        cleaned_datafile = StringIO()
        for line in data.readlines():
            if header is False:
                cleaned_datafile.write(line + '\n')
            elif re.search(_DIVIDER, line) is not None:
                header = False

        cleaned_datafile.seek(0)
        return cleaned_datafile

    def _fix_old_file_paths(self, path):
        if re.match(_EDGAR_RE, path) is None:
            path = _EDGAR + path
        return path

    def read(self):
        if self.symbols == 'full':
            return self._read_one_data(self.url, self.params)

        elif self.symbols == 'daily':
            pass
