import re
import math
from elasticsearch import Elasticsearch
import sys

def writeTxt(fileName,mode,sResult):
    with open(fileName, mode) as f:
        f.write("%s\n" % sResult)

# Fetch Url List 
def addURLList(res1):
    counter=0
    patterns = []
    data = [doc for doc in res1['hits']['hits']]
    for doc in data:
        counter +=1
        pattern = ''
        for part in doc['_source']['Url'].split('/'):
            if re.match('^(\d+)$', part):
                pattern += '/(\d+)'
            elif re.match('(\w+)', part):
                pattern += '/(\w+)'

        if pattern in [x['pattern'] for x in patternsAll]:
            for x in patternsAll:
                if pattern == x['pattern']:
                    x['counter']+=1
        else:
            patternsAll.append({
                'counter': 1,
                'pattern': pattern,
                'sample':doc['_source']['Url']
            })
            patterns.append({
                'pattern': pattern,
                'sample':doc['_source']['Url']
            })

    writeTxt('patternsReultAll.txt','w','')
    for item in patternsAll:
        writeTxt('patternsReultAll.txt','a',item)

    for item in patterns:
        writeTxt('patternsReult.txt','a',item)


patternsAll = []
page_size = 10000
page_counter = 0
indexName='logstash-example-2018.12.11'
es = Elasticsearch([{'host': 'elastic.log.example.com.tr', 'port': 9200}], timeout=5000)
doc = {
        'size' : page_size,
        'query': {
            'match' : {'Status_Code':'200'}
       }
   }


page = es.search(index=indexName, doc_type='doc', body=doc,scroll='1m')
print("%d documents found" % page['hits']['total'])
writeTxt('patternsReult.txt','a','%d documents found' % page['hits']['total'])

sid = page['_scroll_id']
scroll_size = page['hits']['total']
total_pages = math.ceil(scroll_size/page_size)
print('Total items : {}'.format(scroll_size))
writeTxt('patternsReult.txt','a','Total items : {}'.format(scroll_size))
print('Total pages : {}'.format( math.ceil(scroll_size/page_size)))
writeTxt('patternsReult.txt','a','Total pages : {}'.format( math.ceil(scroll_size/page_size)))

# Start scrolling
while (scroll_size > 0):
    # Get the number of results that we returned in the last scroll
    scroll_size = len(page['hits']['hits'])
    if scroll_size>0:
        print('> Scrolling page {} : {} items'.format(page_counter, scroll_size))
        writeTxt('patternsReult.txt','a','> Scrolling page {} : {} items'.format(page_counter, scroll_size))
        try:
            addURLList(page)
        except:
            writeTxt('patternsErros.txt','a','> Scrolling page {} : {} items'.format(page_counter, scroll_size))

    # get next page
    page = es.scroll(scroll_id = sid, scroll = '2m')
    page_counter += 1
    # Update the scroll ID
    sid = page['_scroll_id']
    sys.stdout.flush()

print("--end--")