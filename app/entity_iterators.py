import re
from requests import get
import app.authorities
from app.authorities import to_list
import xmltodict
from app.node_entities import Authority, Record, Photo, Portrait

PRIMO = 'primo.nli.org.il'


class Results:
    def __init__(self, query, count=200):
        self.count = count
        self.query = query
        self.index = 0
        self.page = 1
        self.results = self._get_results()

    @property
    def _search_url(self):
        return 'http://' + PRIMO + '/PrimoWebServices/xservice/search/brief?institution=NNL' \
                                   '&query=any,contains,"{}"&indx={}&bulkSize={}&json=true'

    @property
    def entity_type(self):
        return Record

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < self.count:
            try:
                res = self.results[self.index]
            except IndexError:
                raise StopIteration
            self.index += 1
            return self.entity_type(res)
        self.page += 1
        self.index = 0
        self.results = self._get_results()
        return self.__next__()

    def _get_results(self):
        if self._search().get('DOC'):
            return [item['PrimoNMBib']['record'] for item in to_list(self._search()['DOC'])]
        return []

    def __len__(self):
        return int(self._search()['@TOTALHITS'])

    def _search(self):
        res = get(self._search_url.format(self.query, 1 + (self.page - 1) * self.count, self.count))
        if res.status_code == 500:
            raise StopIteration
        return res.json()['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']


class Photos(Results):
    def __init__(self, query, count=200):
        super().__init__(query, count)

    @property
    def _search_url(self):
        return 'http://primo.nli.org.il/PrimoWebServices/xservice/search/brief?institution=NNL' \
               '&loc=local,scope:(NNL_PIC)&query=title,contains,{}&sortField=&indx={}&bulkSize={}&json=true'

    @property
    def entity_type(self):
        return Photo


class Portraits(Photos):
    def __init__(self, query):
        super().__init__(query, 2)

    @property
    def _search_url(self):
        return 'http://primo.nli.org.il/PrimoWebServices/xservice/search/brief?institution=NNL' \
               '&loc=local,scope:(NNL01_Schwad)&query=title,contains,{}&sortField=&indx={}&bulkSize={}&json=true'

    @property
    def entity_type(self):
        return Portrait


DUMP_PATH = '/home/adir/Downloads/nnl10all.xml'


def get_authorities(from_id=0, to_id=999999999):
    with open(DUMP_PATH) as f:
        buffer = ''
        auth_id = 0
        line = f.readline()
        while not line.strip().endswith('">'):
            line = f.readline()

        for line in f:
            if not auth_id:
                groups = re.match(r'  <controlfield tag="001">(\d*)</controlfield>', line)
                if groups:
                    auth_id = int(groups.group(1))
            buffer += line
            if line.strip() == "</record>":
                if from_id <= auth_id <= to_id:
                    record = xmltodict.parse(buffer)['record']
                    result = {k: record[k] for k in record if k == "controlfield" or k == "datafield"}
                    if result.get('datafield'):
                        yield Authority(app.authorities.convert_dict(result))
                buffer = ''
                auth_id = 0
