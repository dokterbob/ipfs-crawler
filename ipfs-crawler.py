import ipfsApi
api = ipfsApi.Client('127.0.0.1', 5001)

import magic
import requests

hashes = [
    'QmTyPtsw49JV1iFMwtvCVCFP1QwFL4cGsxz2ZtHZJyJY5F',
    'QmQ4EvvCrx2cjY3AWVosfeyy7aKCf3WSVrKomRf1wbvQnW'
]


def crawl_data(data):
    # Identify file based on data
    mimetype = magic.from_buffer(data, mime=True)

    return {
        'mimetype': mimetype
    }


def read_data(resource_hash):
    # Read some data for determining metadata
    from contextlib import closing

    url = 'http://127.0.0.1:8080/ipfs/{0}'.format(resource_hash)
    with closing(requests.get(url, stream=True)) as r:
        data = r.raw.read(1024)

    return data


def crawl_hash(resource_hash, name=None):
    # print("Crawling {0} ({1})".format(resource_hash, name))

    result = api.object_get(resource_hash)

    if result['Data'] == '\x08\x01':
        print("{0} ({1}) is a directory, iterating files".format(
            resource_hash, name
        ))

        for link in result['Links']:
            # crawl_hash(link['Hash'])
            yield from crawl_hash(link['Hash'], link['Name'])

    elif result['Data'][:2] == '\u0008\u0002':
        print("{0} ({1}) is a file, crawling contents".format(
            resource_hash, name
        ))

        data = read_data(resource_hash)

        crawl_result = crawl_data(data)

        stat = api.object_stat(resource_hash)
        crawl_result.update({
            'hash': resource_hash,
            'names': set([name]),
            'size': stat['CumulativeSize']
        })

        yield crawl_result


def main():
    result_set = {}

    for current_hash in hashes:
        results = crawl_hash(current_hash)

        for r in results:
            result_set[r['hash']] = r

    print(result_set)

main()
