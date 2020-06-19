

'''
main indexer object. given number of desired processes, create n specified amount of processes to do the indexes
'''

from CS121_Errors.ProcessLessThanThreeError import ProcessLessThanThreeError
import multiprocessing
import rapidjson
import os
import shutil
import time
from HtmlExtractor import HtmlExtractor
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords, words
import numpy as np
import re
import math
from collections import Counter, OrderedDict, defaultdict
import string
import ijson


class Indexer:

    def __init__(self, to_index: [(str, int)], num_processes=3, tags=None):
        """
        Initializes an instance of Indexer
        :param to_index: The list of file paths to index
        :param num_processes: Number of processes to spin up to index the file system
        :param tags: List of specific tags to look for
        """
        if num_processes < 3:  # just make sure the number of processes is more than two
            raise ProcessLessThanThreeError

        # check to see if tags were specified if so, use specified tags
        if tags:
            self._tags = tags

        # if not, then just use the default ones
        else:
            self._tags = ["h1", "h2"]

        self._to_index = to_index
        self._num_processes = num_processes
        self._remove_index()  # clear the index and ger ready for a new index

    def begin_indexing(self) -> None:
        """
        Start the indexing process and spin up num_processes amount of processes
        :return: None
        """

        # split the array into equally spaced parts
        file_args = np.array_split(self._to_index, self._num_processes)

        # create a list of tuple to pass to each index process
        pool_args = self.create_process_args(file_args, self._tags)

        # spin up processes
        with multiprocessing.Pool() as pool:
            p = pool.starmap(Indexer._index_process, pool_args)

    def get_file_size(self) -> int:
        """
        Gets the total file size given to the indexer
        :return: The total amount of files
        """
        return len(self._to_index)

    @staticmethod
    def create_alphabetical_list(tokens: dict) -> dict:
        """
        given a dict of tokens, create a dictionary of keys separated by their alphabetical ordering
        :param tokens: the dict of tokens
        :return: a dictionary containing the alphabetical ordering of keys
        """
        alphabetical_dict = defaultdict(list)
        # solution for this found here:
        # https://stackoverflow.com/questions/4058967/split-list-of-names-into-alphabetic-dictionary-in-python
        for token in tokens.keys():
            alphabetical_dict[token[0].upper()].append(token)
        return alphabetical_dict

    @staticmethod
    def clear_final_index() -> None:
        """
        Clears the final index directory for the creation of a new index
        :return: None
        """
        for (root, dirs, files) in os.walk("final_index/"):
            for file in files:
                with open(root+file, 'w') as fp:
                    rapidjson.dump({}, fp)

    @staticmethod
    def consolidate(path: str) -> None:
        """
        Consolidates the newly created index and splits them into alphabetical order
        :param path: the path to the partial indexes to consolidate
        :return: None
        """
        index_split = 4 # parts to split the index into

        # get the a-z value
        atz = [i for i in string.ascii_uppercase]
        total_atz = set(i for i in string.ascii_uppercase)
        atz = np.array_split(atz, index_split)  # split each alphabet into 4 pieces each 7 letters long

        # turn the atz split into a list of sets to make search times O(1)
        atz = [set(i) for i in atz]

        atz.append(set(['1', '2', '3', '4', '5', '6', '7', '8', '9']))
        # list of open files to index
        print(atz)
        files_to_index = []

        # lists
        atog = hton = otot = utoz = oneto9 = defaultdict(list)
        Indexer.clear_final_index()

        counter = 0
        # iterate through the files with the indexes
        try:
            files_to_index = Indexer.get_files(path)
            for i in range(len(files_to_index)):
                print(i)
                atog = defaultdict(list)
                hton = defaultdict(list)
                otot = defaultdict(list)
                utoz = defaultdict(list)
                oneto9 = defaultdict(list)
                #atog = hton = otot = utoz = oneto9 = defaultdict(list)
                # gets the string ranges based off the
                tokens = rapidjson.load(files_to_index[i])
                print(files_to_index[i])
                alphabetical_tokens = Indexer.create_alphabetical_list(tokens)
                counter += len(tokens)
                for alpha_token in alphabetical_tokens:
                    # if in range a to g
                    if alpha_token in atz[0]:
                        # add to a-g dict
                        #print(Indexer.create_separated_dict(alphabetical_tokens[alpha_token], tokens))
                        atog = Indexer.merge_dictionaries(atog, Indexer.create_separated_dict(
                            alphabetical_tokens[alpha_token], tokens))
                    if alpha_token in atz[1]:
                        # add to h-n dict
                        hton = Indexer.merge_dictionaries(hton, Indexer.create_separated_dict(
                            alphabetical_tokens[alpha_token], tokens))
                    if alpha_token in atz[2]:
                        # add to o-d dict
                        otot = Indexer.merge_dictionaries(otot, Indexer.create_separated_dict(
                            alphabetical_tokens[alpha_token], tokens))
                    if alpha_token in atz[3]:
                        # add to u-z dict
                        utoz = Indexer.merge_dictionaries(utoz, Indexer.create_separated_dict(
                            alphabetical_tokens[alpha_token], tokens))
                    else:
                        # add to alphanum dict
                        oneto9 = Indexer.merge_dictionaries(oneto9, Indexer.create_separated_dict(
                            alphabetical_tokens[alpha_token], tokens))

                # finally write to the index
                Indexer._write_to_index('final_index/atog.json', atog)
                Indexer._write_to_index('final_index/hton.json', hton)
                Indexer._write_to_index('final_index/otot.json', otot)
                Indexer._write_to_index('final_index/utoz.json', utoz)
                Indexer._write_to_index('final_index/oneto9.json', oneto9)

        finally:
            for file in files_to_index:
                file.close()

    @staticmethod
    def _truncate(to_trunc: float, decimal_place: int) -> float:
        """
        Truncates the given value to the decumal place
        solution found here: https://stackoverflow.com/questions/8595973/truncate-to-three-decimals-in-python
        :param to_trunc: the float to truncate
        :param decimal_place: the value to round the decimal place to
        :return: the truncated number
        """
        return math.trunc(to_trunc*decimal_place) / (10.0**decimal_place)

    @staticmethod
    def finish_ranking(path: str, total_size: int) -> None:
        """
        Finish the idf ranking
        :param path: The path to finish the idf ranking for the indexes
        :param total_size: the total size of all the document
        :return: None
        """
        to_finish = dict()
        for (root, dirs, files) in os.walk(path):
            for file in files:
                print("indexing: {}".format(file))
                with open(root+file, 'r') as fp:
                    to_finish = rapidjson.loads(fp.read())
                    for token in to_finish.keys():
                        doc_len = len(to_finish[token])
                        for index in to_finish[token]:
                            # get the tf value
                            idf = math.log(total_size/doc_len, 2)
                            index[1] *= idf
                            index[1] = round(index[1], 3)
                            #index[1] = Indexer._truncate(math.log(total_size/doc_len, 2), 5)
                with open(root+file, 'w') as fp:
                    rapidjson.dump(to_finish, fp)

    @staticmethod
    def _write_to_index(path: str, to_write: dict) -> None:
        # need to open file, merge the two dicts, and off load the newly merged value
        print(path)
        read_info = {}
        with open(path, 'r') as fp:
            try:
                read_info = rapidjson.load(fp)
                # merge the read and the value to write
                read_info = Indexer.merge_dictionaries(read_info, to_write)
            # if document is empty don't do anything
            except ValueError:
                pass

        with open(path, 'w') as fp:
            rapidjson.dump(read_info, fp)

    @staticmethod
    def merge_dictionaries(x: dict, y: dict) -> dict:
        """
        Given two dictionaries, merge them into one
        Solution found here: https://stackoverflow.com/questions/33931259/to-merge-two-dictionaries-of-list-in-python
        :param x: First dictionary to merge
        :param y: Second dictionary to merge
        :return: Product of both dictionaries merged
        """
        merged = x
        for k, v in y.items():
            if k in merged.keys():
                merged[k] += v
            else:
                merged[k] = v
        return merged

    @staticmethod
    def create_separated_dict(keys_to_separate: [], to_extract_from: {}) -> dict:
        """
        Given a set of keys to separate from the base dictionary, get all the keys and their corresponding values
        and put them into a new dictionary
        :param keys_to_separate: The set of key to separate
        :param to_extract_from: The base dictionary to search for the keys and their values
        :return: A dict of the extracted values
        """
        extracted_values = defaultdict()
        for key in keys_to_separate:
            extracted_values[key] = to_extract_from[key]
        return extracted_values

    @staticmethod
    def get_files(dir: str) -> list:
        """
        Given a directory, open each file and return a list of all the open file streams
        :param dir: The directory of which to search for files to open
        :return: a list of open files
        """
        try:
            to_index = []
            for (root, dirs, files) in os.walk(dir):
                for file in files:
                    to_index.append(open(root+file, 'r'))
            return to_index

        except FileNotFoundError:
            print("Indexer.get_files error. Error opening file. Please try again.")
            raise FileNotFoundError

    @staticmethod
    def _index_process(to_index: [(str, int)], tags: [str], process_id: int) -> None:
        """
        One process of indexer
        :return: None
        """
        try:
            print("Index process #{} has started.".format(process_id))
            begin = time.time()
            html_extractor = HtmlExtractor(tags)
            index = {}  # index fragment
            stopword = set([i.lower() for i in stopwords.words('english')])
            valid_words = set([i.lower() for i in words.words()])
            for file in to_index:
                with open(file[0]) as fp:

                    # load the json file
                    read_json = rapidjson.load(fp)

                    # tokenize the html file
                    extracted = html_extractor.extract_tags(read_json['content'])
                    #print(extracted)

                    # now tokenize the values based on weighted and unweighted values
                    weighted = Indexer._tokenize(extracted[0], stopword, valid_words)
                    unweighted = Indexer._tokenize(extracted[1], stopword, valid_words)
                    #print("unweighted {} weighted {}".format(unweighted, weighted))
                    # now get the frequencies
                    weighted_freq = Indexer._get_frequency(weighted)
                    unweighted_freq = Indexer._get_frequency(unweighted)
                    #return None
                    #print("unweighted {} weighted {}".format(unweighted_freq.keys(), weighted_freq.keys()))

                    # merge both dicts for general frequency dict
                    # solution found here:
                    # https://www.geeksforgeeks.org/python-combine-two-dictionary-adding-values-for-common-keys/
                    total_freq = Counter(weighted_freq) + Counter(unweighted_freq)
                    #print("TOTAL FREQ ID:{} FREQ: {}".format(process_id, total_freq.keys()))
                    # now iterate through each token in total_freq and get the tfdif value
                    for token in total_freq:
                        tf = math.log(total_freq[token], 10) + 1  # get the td value
                        if token in weighted_freq:
                            tf += 1.75  # add an extra weight to signify more importance

                        # check to see if the value
                        if token not in index:
                            index[token] = [(file[1], tf)]
                        else:
                            index[token].append((file[1], tf)) # now store the token onto the index
            # make the index alphabetical
            temp_index = OrderedDict()
            for key in sorted(index.keys()):
                temp_index[key] = index[key]
            index = temp_index

            print("==================================")
            with open("indexes/index_#{}.json".format(process_id), 'w') as partial_index_file:
                rapidjson.dump(index, partial_index_file)

            print("[DONE] Index process #{} has finished. Process took {:.2f} minutes.".format(
                process_id, (time.time() - begin)/60))

        # make sure there is no memory error if there is just quit
        except MemoryError:
            print("The program encountered a memory error. Exiting.")
            return

    def create_process_args(self, to_process: [(str, int)], tags: [str]) -> [()]:
        """
        Given a list of arguments, create a list of tuples for the arguments for each process to use
        :param to_process: file paths of the html that the user wants to process
        :param tags: the list of tags that are weighted
        :return: a list of tuples to pass to the index process
        """
        args = []
        for i in range(self._num_processes):
            args.append((to_process[i], tags, i))

        return args

    @staticmethod
    def index_documents(to_index: []) -> None:
        """
        Given a set of documents, opens a document index file and gives each file
        a unique doc id number. After indexing each file, write given value to document_indexes.json
        :param to_index: The files of which to index.
        :return: None
        """

        start_time = time.time()
        print("[...] Generating doc IDs...")
        current_doc = {}
        counter = 0
        with open("document_indexes.json", 'w') as docs_file:
            for file in to_index:  # for every valid file, give each file a document id number
                current_doc[file] = counter
                counter += 1
            rapidjson.dump(current_doc, docs_file)  # after each file has an id number, write it to document_indexes
        print(len(current_doc))  # total number of documents indexed
        print("[DONE] Doc indexing took {:.2f} secs".format(time.time() - start_time))
        print()

    @staticmethod
    def _remove_index() -> None:
        """
        Removes any pre-existing  index
        :return: None
        """
        if os.path.exists('indexes/'):  # check to see if indexes has already been made
            shutil.rmtree('indexes')
        os.mkdir("indexes/")  # if not index directory exists, make a new empty one

    @staticmethod
    def _tokenize(to_tokenize: str, stopword: {str}, valid_words: {str}) -> [str]:
        """
        Given a str, tokenize and stem the words, returns a list of tokenized words
        :param to_tokenize:
        :return: a list of tokenized words
        """
        cleaned = []
        # using the nltk word tokenizer, tokenize each value given
        tokenized = word_tokenize(to_tokenize)
        #print("TOKENS {}".format(tokenized))

        # iterate through each token and add if it matches the regex
        for token in tokenized:
            token = token.lower()
            #print(token)
            #matched = re.findall("([a-zA-Z0-9]+)", token)
            if token not in stopword and token in valid_words:
                cleaned.append(token)
            elif token.isnumeric() and len(token) < 5:
                cleaned.append(token)
        return cleaned

    @staticmethod
    def _get_frequency(tokens: [str]) -> {str: int}:
        """
        Given a list of tokens, count the frequency of the said tokens within the list of tokenized words
        :param tokens: a list of tokens to count the frequencies
        :return: a dictionary with the frequency list
        """
        frequency_list = {}

        for token in tokens:
            if token not in frequency_list.keys():
                frequency_list[token] = 1
            else:
                frequency_list[token] += 1
        #print("frequency list {}".format(frequency_list))
        return frequency_list