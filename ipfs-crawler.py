import ipfsApi
api = ipfsApi.Client('127.0.0.1', 5001)

import subprocess
import json
import argparse
import asyncio

hashes = [
    'QmbyLYvJ43xyUmrU5A2Ye4ZiPHXWmB4j5nYRSpiTLBhbtn',
    'QmTyPtsw49JV1iFMwtvCVCFP1QwFL4cGsxz2ZtHZJyJY5F',
    'QmQ4EvvCrx2cjY3AWVosfeyy7aKCf3WSVrKomRf1wbvQnW'
]

import elasticsearch
es = elasticsearch.Elasticsearch()

q = asyncio.Queue()


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

    process = yield from asyncio.create_subprocess_exec('tika', '-j', ipfs_path, stdout=asyncio.subprocess.PIPE)

    # Wait for results
    yield from process.wait()

    output = yield from process.stdout.read()
    parsed_results = json.loads(output.decode("utf-8"))

    return parsed_results


def crawl_hash(resource_hash, name=None, parent_hash=None):
    print("Crawling {0} ({1})".format(resource_hash, name))

    # Check for existing items. Note: exists() without doc_type didn't work

    # if es.exists(index='ipfs', id=resource_hash, doc_type='_all'):
    #     print('{0} ({1}): Already indexed.'.format(resource_hash, name))
    #     return

    result = api.object_get(resource_hash)

    if result['Data'] == '\x08\x01':
        print("{0} ({1}) is a directory, iterating files".format(
            resource_hash, name
        ))

        for link in result['Links']:
            # crawl_hash(link['Hash'])
            # crawl_hash(link['Hash'], link['Name'], resource_hash)
            q.put_nowait([link['Hash'], link['Name'], resource_hash])


    elif result['Data'][:2] == '\u0008\u0002':
        print("{0} ({1}) is a file, crawling contents".format(
            resource_hash, name
        ))

        crawl_result = yield from crawl_data(resource_hash)
        stat = api.object_stat(resource_hash)

        crawl_result.update({
            'names': [name] if name else [],
            'parents': [resource_hash] if resource_hash else [],
            'size': stat['CumulativeSize']
        })

        add_result(resource_hash, crawl_result)


@asyncio.coroutine
def crawl_hashes(worker_number):
    while not q.empty():
        queue_item = yield from q.get()
        print('Worker {0} started with: {1} ({2} items left)'.format(
            worker_number, queue_item[0], q.qsize()))
        yield from crawl_hash(*queue_item)


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
        q.put_nowait([current_hash])

    loop = asyncio.get_event_loop()

    tasks = [crawl_hashes(worker_number) for worker_number in range(8)]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

    # Perform test search
    res = es.search(index="ipfs", body={"query": {"match_all": {}}})

    import pprint
    pp = pprint.PrettyPrinter(width=41, compact=True)

    # pp.pprint(res)

main()
