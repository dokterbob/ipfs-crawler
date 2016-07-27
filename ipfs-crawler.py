import ipfsApi
api = ipfsApi.Client('127.0.0.1', 5001)

import magic
import requests

hashes = [
    'QmTyPtsw49JV1iFMwtvCVCFP1QwFL4cGsxz2ZtHZJyJY5F',
    'QmQ4EvvCrx2cjY3AWVosfeyy7aKCf3WSVrKomRf1wbvQnW'
]


crawl_results = {}
def add_result(resource_hash, data):
    """ Add crawler result. """

    if resource_hash in crawl_results:
        # Concatenate names, parents and parent_names

        def update_entry(name):
            data.update({
                name: data[name].union(crawl_results[resource_hash][name])
            })

        update_entry('names')
        update_entry('parents')

    crawl_results[resource_hash] = data


def crawl_data(data):
    # Identify file based on data
    mimetype = magic.from_buffer(data, mime=True)

    main_type, sub_type = mimetype.split('/', 2)

    print(main_type, sub_type)
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


def crawl_hash(resource_hash, name=None, parent_hash=None):
    # print("Crawling {0} ({1})".format(resource_hash, name))

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

        data = read_data(resource_hash)

        crawl_result = crawl_data(data)

        stat = api.object_stat(resource_hash)

        add_result(
            resource_hash,
            {
                'names': set([name]),
                'parents': set([resource_hash]),
                'size': stat['CumulativeSize']
            }
        )


def main():
    result_set = {}

    for current_hash in hashes:
        crawl_hash(current_hash)

    import pprint
    pp = pprint.PrettyPrinter(width=41, compact=True)

    pp.pprint(crawl_results)

main()
