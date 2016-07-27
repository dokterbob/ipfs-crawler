import ipfsApi
api = ipfsApi.Client('127.0.0.1', 5001)

import subprocess
import json

hashes = [
    'QmbyLYvJ43xyUmrU5A2Ye4ZiPHXWmB4j5nYRSpiTLBhbtn',
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
                name: list(set(data[name] + crawl_results[resource_hash][name]))
            })

        update_entry('names')
        update_entry('parents')

    crawl_results[resource_hash] = data


def crawl_data(resource_hash):
    # Identify file based on data

    ipfs_path = '/ipfs/{0}'.format(resource_hash)
    result = subprocess.run(['tika', '-j', ipfs_path],
        check=True, stdout=subprocess.PIPE, universal_newlines=True)

    parsed_results = json.loads(result.stdout)

    return parsed_results


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

        crawl_result = crawl_data(resource_hash)

        stat = api.object_stat(resource_hash)

        crawl_result.update({
            'names': [name],
            'parents': [resource_hash],
            'size': stat['CumulativeSize']
        })

        add_result(resource_hash, crawl_result)


def main():
    result_set = {}

    for current_hash in hashes:
        crawl_hash(current_hash)

    import pprint
    pp = pprint.PrettyPrinter(width=41, compact=True)

    pp.pprint(crawl_results)

main()
