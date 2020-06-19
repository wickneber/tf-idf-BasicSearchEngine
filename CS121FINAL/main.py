
import os
import json
import time
import multiprocessing
import numpy as np
import hashlib
from HtmlExtractor import HtmlExtractor
import rapidjson
import Indexer
import argparse
from Search_Engine import SearchEngine


def extract_files(directory: str) -> []:
    '''
    Given a directory to extract from, get the json files with utf-8 or ascii encodings and put them into a
    path list

    :param directory: the directory to extract the json files from
    :return: the list of all the files that are encoded in either ascii or utf-8
    '''
    extracted_files = []
    print("[...] Extracting files")
    for (root, dirs, files) in os.walk(directory):
        for file in files:
            fullpath = '{}/{}'.format(root, file)
            extracted_files.append(fullpath)
    print("[+++] Extracted {} files".format(len(extracted_files)))
    print("[DONE] Extracting finished")
    return extracted_files


def cleaner_process(to_clean: [], id: int, shared_memory: dict, verbose=False) -> []:
    """
    One process instance that is called by multiprocessing, clean given files and remove any duplicates or undesired
    file encodings
    :param to_clean: The list of file paths to clean
    :param id: The process id number
    :param shared_memory: A shared memory dict that stores all the approved file paths
    :param: verbose: A boolean value determining if the user wants the cleaning to be verbose or not
    :return: A list of all the approved file paths
    """
    cleaned = []
    if verbose:
        print("[...] List size: {}".format(len(to_clean)))
        print("[...] Cleaner process #{} has started".format(id))
        print()

    process_begin = time.time()
    for file in to_clean:
        with open(file) as fp:
            loaded_file = rapidjson.load(fp)
            site_checksum = hashlib.md5(loaded_file["content"].encode("utf-8")).hexdigest()
            if loaded_file['encoding'] in ['ascii', 'utf-8'] and site_checksum not in shared_memory:
                cleaned.append(file)
                shared_memory[site_checksum] = 1

    if verbose:
        print("[+++] Process #{} took {:.2f} seconds.".format(id, time.time() - process_begin))
        print("[DONE] Cleaner process #{} has finished".format(id))
        print()

    return cleaned


def clean_files(to_clean: [], processes: int) -> []:
    if processes < 1:
        return None

    # split the list into evenly pieced chunks
    to_clean_args = np.array_split(to_clean, processes)

    # generate the arguments for the pool
    pool_args = []
    shared_dict = multiprocessing.Manager().dict()
    for i in range(processes):
        pool_args.append((to_clean_args[i], i, shared_dict))

    with multiprocessing.Pool() as pool:
        p = pool.starmap(cleaner_process, pool_args)

    return flatten_list(p)


def flatten_list(to_flatten: list) -> list:
    """
    Given a list flatten it. Note: Only flattens to one dimension deep
    :param to_flatten: The list to flatten
    :type to_flatten: List
    :return: A flattened list
    :rtype: List
    """
    print("[...] Flattening list")
    begin = time.time()
    flattened = []
    for i in to_flatten:
        if type(i) == list:
            flattened.extend(i)
    print("[Done] Flattening took {:.3f} seconds".format(time.time() - begin))
    print()
    return flattened


def index_documents(files: list) -> [(str, int)]:
    """
    Given a set of documents, opens a document index file and gives each file
    a unique doc id number.
    :param files: The files of which to index.
    :return: a list of tuple with file paths with corresponding id number as values
    """
    start_time = time.time()
    print("[...] Generating doc IDs...")
    current_doc = {}
    counter = 0
    return_doc = []
    with open("document_indexes.json", 'w') as docs_file:
        for file in files: # for every valid file, give each file a document id number
            current_doc[counter] = file
            return_doc.append((file, counter))
            counter += 1
        json.dump(current_doc, docs_file) # after each file has an id number, write it to document_indexes
    print("[DONE] Doc indexing took {:.2f} secs. Indexed {} documents.".format(time.time() - start_time, len(current_doc)))
    print()
    return return_doc


if __name__ == "__main__":
    #'''
    # first, parse the arguments
    parse_command = argparse.ArgumentParser(description="Rudimentary search engine.")
    parse_command.add_argument("--index", '-i', action='store_true', default=False)
    parse_command.add_argument("--search",'-s', nargs="+", type=str)
    args = parse_command.parse_args()

    index_opt = args.index
    print("INDEX OPT {}".format(index_opt))
    search_value = None



    extractor = HtmlExtractor(['h1', 'h2', 'b', 'i'])
    with open('test.html.txt') as fp:
        print(extractor.extract_tags(fp.read()))

    if args.search:
        search_value = " ".join(args.search)
    # if the user wants to index the values
    if index_opt:

        begin = time.time()
        extracted = extract_files('DEV/')
        processes = 500
        cleaned = clean_files(extracted, processes)
        docs = index_documents(cleaned)

        Indexer.Indexer.finish_ranking('final_index/', len(cleaned))

        #extractor = HtmlExtractor(['h1', 'h2', 'b', 'i'])
        
        print("Indexing")
        indexer = Indexer.Indexer(docs, 10, ['h1', 'h2', 'h3', 'title', 'b', 'i'])
        indexer.begin_indexing()
        print("Consolidating...")
        Indexer.Indexer.consolidate('indexes/')
        print("Ranking...")
        Indexer.Indexer.finish_ranking('final_index/', len(cleaned))

        #indexer = Indexer.Indexer(docs, 7, ['h1', 'h2'])
        #indexer.begin_indexing()
        print("Total Time: {:.2f} minutes.".format((time.time() - begin)/60))

    if search_value:
        #TODO use the search engine class to search the terms
        engine = SearchEngine("final_index/")
        docs = engine.search(search_value, "document_indexes.json")

        print("Documents found: ")
        for doc in docs:
            print("==> {}".format(doc))

    
    """
    Indexer.Indexer.consolidate('indexes/')
    x = {"x":[(1,2), (9,9)], 'y':[(2,2)]}
    y = {'x':[(7,7)], 'y':[(3,3)]}
    print(Indexer.Indexer.merge_dictionaries(x, y))
    """
