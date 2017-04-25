import re
from time import sleep
from requests import get
import app.authorities
from app.authorities import to_list
import xmltodict
from app.node_entities import Authority, Record, Photo, Portrait
from app.settings import DUMP_PATH
import py2neo
from app.settings import *

# PRIMO = 'primo.nli.org.il'
PRIMO = '178.62.194.73'
PAGED_CYPHER = "{} skip {} limit {}"

class N4JQuery:
    def __init__(self, cypher_query, page_size=200, offset=0, limit=0):
        self.count = page_size
        self.offset = offset
        self.limit = limit
        self.cypher = cypher_query
        self.index = 0
        self.page = 1
        self.graph = self._graph()
        self.results = self._get_results()

    def _graph(self):
        py2neo.authenticate(NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD)
        return py2neo.Graph('http://' + NEO4J_URL + NEO4J_GRAPH)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < self.count:
            try:
                res = self.results[self.index]
            except IndexError:
                raise StopIteration
            self.index += 1
            return res
        self.page += 1
        self.index = 0
        self.results = self._get_results()
        return self.__next__()

    def _get_results(self):
        res = self._query()
        if res:
            return res
        return []

    def _query(self):
        retries = 0
        while True:
            try:
                skip = self.count * (self.page - 1)
                query = PAGED_CYPHER.format(self.cypher, self.offset + skip, self.count)
                print(query)
                res = self.graph.cypher.execute(query)
            except:
                if retries > 10:
                    raise StopIteration
                retries += 1
                print('connection issue...')
                sleep(5)
                continue
            break
        if retries:
            print('all fine!')
        # TODO: other stop condition
        return res

    def __len__(self):
        return len(self._query())

class Results:
    def __init__(self, query, max_results=200):
        self.count = max_results
        self.query = query
        self.index = 0
        self.page = 1
        self.results = self._get_results()

    @property
    def _search_url(self):
        url = 'http://' + PRIMO + '/PrimoWebServices/xservice/search/brief?institution=NNL' \
                                   '&query=any,contains,"{}"&indx={}&bulkSize={}&json=true'
        return url

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
        search = self._search()
        if search.get('DOC'):
            return [item['PrimoNMBib']['record'] for item in to_list(search['DOC']) if
                    item.get('PrimoNMBib') and item['PrimoNMBib'].get('record')]
        return []

    def __len__(self):
        return int(self._search()['@TOTALHITS'])

    def _search(self):
        retries = 0
        while True:
            try:
                url = self._search_url.format(self.query, 1 + (self.page - 1) * self.count, self.count)
                print(url)
                res = get(url)
                sleep(4)
            except Exception as ex:
                if retries > 10:
                    raise ex
                retries += 1
                print('connection issue...', ex)
                sleep(5)
                continue
            break
        if retries:
            print('all fine!')
        if res.status_code == 500:
            raise StopIteration
        try:
            return res.json()['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']
        except:
            print('oh.')
            raise StopIteration


class Photos(Results):
    def __init__(self, query, max_results=200):
        super().__init__(query, max_results)

    @property
    def _search_url(self):
        return 'http://' + PRIMO + '/PrimoWebServices/xservice/search/brief?institution=NNL' \
               '&loc=local,scope:(NNL_PIC)&query=facet_topic,exact,"{} M"&sortField=&indx={}&bulkSize={}&json=true'

    @property
    def entity_type(self):
        return Photo


class Portraits(Photos):
    def __init__(self, query):
        super().__init__(query, 2)

    @property
    def _search_url(self):
        return 'http://' + PRIMO + '/PrimoWebServices/xservice/search/brief?institution=NNL' \
               '&loc=local,scope:(NNL01_Schwad)&query=facet_topic,exact,"{}"&sortField=&indx={}' \
               '&bulkSize={}&json=true'

    @property
    def entity_type(self):
        return Portrait


def get_authorities(from_id=0, to_id=999999999, list_authorities=[]):
    with open(DUMP_PATH, encoding='utf8') as f:
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
                if from_id <= auth_id <= to_id and (len(list_authorities) == 0 or auth_id in list_authorities):
                    record = xmltodict.parse(buffer)['record']
                    result = {k: record[k] for k in record if k == "controlfield" or k == "datafield"}
                    if result.get('datafield'):
                        try:
                            yield Authority(app.authorities.convert_dict(result))
                        except:
                            pass
                elif auth_id > to_id:
                    break
                buffer = ''
                auth_id = 0
