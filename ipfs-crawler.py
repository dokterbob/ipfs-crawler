import ipfsApi
api = ipfsApi.Client('127.0.0.1', 5001)

import subprocess
import json
import argparse

hashes = [
    'QmbyLYvJ43xyUmrU5A2Ye4ZiPHXWmB4j5nYRSpiTLBhbtn',
    'QmTyPtsw49JV1iFMwtvCVCFP1QwFL4cGsxz2ZtHZJyJY5F',
    'QmQ4EvvCrx2cjY3AWVosfeyy7aKCf3WSVrKomRf1wbvQnW'
]

import elasticsearch
es = elasticsearch.Elasticsearch()


def add_result(resource_hash, data):
    """ Add crawler result. """

    assert 'Content-Type' in data
    doc_type = data['Content-Type'].split('/',1)[0]

    try:
        # Concatenate names, parents and parent_names
        res = es.get(index='ipfs', id=resource_hash, doc_type=doc_type)

        def update_entry(name):
            data.update({
                name: list(set(data[name] + res['_source'][name]))
            })

        update_entry('names')
        update_entry('parents')

    except elasticsearch.exceptions.NotFoundError:
        pass

    es.index(index='ipfs', doc_type=doc_type, id=resource_hash, body=data)


def crawl_data(resource_hash):
    # Identify file based on data

    ipfs_path = '/ipfs/{0}'.format(resource_hash)
    result = subprocess.run(['tika', '-j', ipfs_path],
        check=True, stdout=subprocess.PIPE, universal_newlines=True)

    parsed_results = json.loads(result.stdout)

    return parsed_results


def crawl_hash(resource_hash, name=None, parent_hash=None):
    # print("Crawling {0} ({1})".format(resource_hash, name))

    # Check for existing items. Note: exists() without doc_type didn't work
    try:
        es.get(index='ipfs', id=resource_hash)

        print('{0} ({1}): Already indexed.'.format(resource_hash, name))
        return
    except elasticsearch.exceptions.NotFoundError:
        pass

    result = api.object_get(resource_hash)

    if result['Data'] == '\x08\x01':
        print("{0} ({1}) is a directory, iterating files".format(
            resource_hash, name
        ))

        for link in result['Links']:
            # crawl_hash(link['Hash'])
            crawl_hash(link['Hash'], link['Name'], resource_hash)

    elif result['Data'][:2] == '\u0008\u0002':
        print("{0} ({1}) is a file, crawling contents".format(
            resource_hash, name
        ))

        crawl_result = crawl_data(resource_hash)
        stat = api.object_stat(resource_hash)

        crawl_result.update({
            'names': [name] if name else [],
            'parents': [resource_hash] if resource_hash else [],
            'size': stat['CumulativeSize']
        })

        add_result(resource_hash, crawl_result)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Crawl IPFS hashes.')
    parser.add_argument('hashes', nargs='+')

    args = parser.parse_args()

    # Assure index exists
    ic = elasticsearch.client.IndicesClient(es)

    if not ic.exists('ipfs'):
        ic.create('ipfs')

    # Crawling
    for current_hash in args.hashes:
        crawl_hash(current_hash)

    # Perform test search
    res = es.search(index="ipfs", body={"query": {"match_all": {}}})

    import pprint
    pp = pprint.PrettyPrinter(width=41, compact=True)

    pp.pprint(res)

main()
