import re
import datetime as dt
from ftplib import FTP
import gzip

from pandas import read_csv
from pandas import DataFrame
from pandas.io.common import ZipFile
from pandas.compat import StringIO
from pandas.compat import BytesIO

from pandas_datareader.base import _BaseReader


_URL_FULL = 'ftp://ftp.sec.gov/edgar/full-index/master.zip'
_URL_DAILY = 'ftp://ftp.sec.gov/'
_SEC_FTP = FTP('ftp.sec.gov')

_COLUMNS = ['cik', 'company_name', 'form_type', 'date_filed', 'filename']
_DIVIDER = re.compile('--------------')
_EDGAR = 'edgar/'
_EDGAR_DAILY = 'edgar/daily-index'
_EDGAR_RE = re.compile(_EDGAR)
_EDGAR_MIN_DATE = dt.datetime(1994, 7, 1)
_ZIP_RE = re.compile('\.zip$')
_GZ_RE = re.compile('\.gz$')

_MLSD_VALUES_RE = re.compile('modify=(?P<modify>.*?);.*'
                             'type=(?P<type>.*?);.*'
                             '; (?P<name>.*)$')
_FILENAME_DATE_RE = re.compile('\w*?\.(\d*)\.idx')
_FILENAME_MASTER_RE = re.compile('master\.\d*\.idx')
_EDGAR_MAX_6_DIGIT_DATE = dt.datetime(1998, 5, 15)


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

    def _read_gzfile(self, url):

        zipf = BytesIO(self._get_response(url).content)
        zf = gzip.open(zipf, 'rb')
        try:
            data = zf.read().decode()
        finally:
            zf.close()

        return StringIO(data)

    def _read_one_data(self, url, params):

        if re.search(_ZIP_RE, url) is not None:
            index_file = self._read_zipfile(url)
        elif re.search(_GZ_RE, url) is not None:
            index_file = self._read_gzfile(url)
        else:
            index_file = StringIO(self._get_response(url).content.decode())

        index_file = self._remove_header(index_file)
        index = read_csv(index_file, delimiter='|', header=None,
                         index_col=False, names=_COLUMNS,
                         low_memory=False)
        index['filename'] = index['filename'].map(self._fix_old_file_paths)
        return index

    def _read_daily_data(self, url, params):
        doc_index = DataFrame()
        file_index = self._get_dir_lists()
        for idx_entry in file_index:
            if self._check_idx(idx_entry):
                daily_idx_url = (self.url + idx_entry['path'] + '/' +
                                 idx_entry['name'])
                daily_idx = self._read_one_data(daily_idx_url, params)
                doc_index = doc_index.append(daily_idx)
        return doc_index

    def _check_idx(self, idx_entry):
        if re.match(_FILENAME_MASTER_RE, idx_entry['name']):
            if idx_entry['date'] is not None:
                if (self.start <= idx_entry['date'] <= self.end):
                    return True
        else:
            return False

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
            return self._read_daily_data(self.url, self.params)

    def _sanitize_dates(self, start, end):
        start_none = start is None
        end_none = end is None

        start, end = super()._sanitize_dates(start, end)

        if start_none:
            start = dt.datetime(2015, 1, 1)
        if end_none:
            end = dt.datetime(2015, 1, 3)
        if start < _EDGAR_MIN_DATE:
            start = _EDGAR_MIN_DATE

        return start, end

    def _get_dir_lists(self):
        _SEC_FTP.login()
        mlsd_tree = self._get_mlsd_tree(_EDGAR_DAILY)
        _SEC_FTP.close()
        return mlsd_tree

    def _get_mlsd_tree(self, dir, top=True):
        initial_mlsd = self._get_mlsd(dir)
        mlsd = initial_mlsd[:]
        for entry in initial_mlsd:
            if entry['type'] == 'dir':
                if top is True:
                    if self._check_mlsd_year(entry) is not True:
                        continue
                subdir = dir + '/' + entry['name']
                mlsd.extend(self._get_mlsd_tree(subdir, False))
        return mlsd

    def _get_mlsd(self, dir):
        dir_list = []
        _SEC_FTP.retrlines('MLSD' + ' ' + dir, dir_list.append)

        dict_list = []
        for line in dir_list:
            entry = self._process_mlsd_line(line)
            entry['path'] = dir
            dict_list.append(entry)

        return dict_list

    def _process_mlsd_line(self, line):
        line_dict = re.match(_MLSD_VALUES_RE, line).groupdict()
        line_dict['date'] = self._get_index_date(line_dict['name'])
        return line_dict

    def _get_index_date(self, filename):
        try:
            idx_date = re.search(_FILENAME_DATE_RE, filename).group(1)
            if len(idx_date) == 6:
                if idx_date[-2:] == '94':
                    filedate = dt.datetime.strptime(idx_date, '%m%d%y')
                else:
                    filedate = dt.datetime.strptime(idx_date, '%y%m%d')
                    if filedate > _EDGAR_MAX_6_DIGIT_DATE:
                        filedate = None
            elif len(idx_date) == 8:
                filedate = dt.datetime.strptime(idx_date, '%Y%m%d')
        except AttributeError:
            filedate = None

        return filedate

    def _check_mlsd_year(self, entry):
        try:
            if (self.start.year <= int(entry['name']) <= self.end.year):
                return True
            else:
                return False
        except TypeError:
            return False
