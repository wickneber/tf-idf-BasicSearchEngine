from nltk import word_tokenize, PorterStemmer
import string
import numpy as np
import os
import time
import json
import ujson
from nltk.corpus import stopwords, words
from nltk import PorterStemmer



class SearchEngine:

    def __init__(self, path: str):
        self._search__path = path
        self._opened_files = set()

    def search(self, to_search: str, document_index_path: str) -> [str]:
        """
        Given a query to search, search the indexes and get the corresponding highest raking values from the document
        indexes
        :param document_index_path: path to the document indexes
        :param to_search: the string to search
        :return: a list of the highest ranking documents
        """
        begin = time.time()
        # first tokenize the values to get each value to search
        tokenized_query = SearchEngine._clean_query(word_tokenize(to_search))
        tokenized_query = sorted(tokenized_query)
        open_files = {}
        found_docs = {}

        try:
            open_files = SearchEngine.open_file(self._search__path)
            temp_load = {}
            prev_cat = ""
            print("QUERY {}".format(tokenized_query))
            found_token = ""
            for token in tokenized_query:
                try:
                    token = token.lower()
                    # get the category of which the value is to be
                    category = SearchEngine._check_category(token)

                    begin_t = time.time()
                    # load the corresponding index into memory

                    print("====> {}".format(open_files[category]))
                    if prev_cat != category:
                        prev_cat = category
                        doc = SearchEngine._read_json(open_files[category], token)
                        #doc = ujson.loads(open_files[category].read())
                        #open_files[category].seek(0)
                        found_token = doc[token]
                        #found_token = ijson.items(open_files[category], token)
                        """
                        for prefix, event, value in ijson.parse(open_files[category]):
                            print("prefix {} event {} value {}".format(prefix, event, value))
                        """
                        #found_docs[token] = found_token
                        #print(str(read_file))
                        #for i in read_file:
                            #print(i)
                        #temp_load = ujson.loads(read_file)
                        #open_files[category].seek(0)
                        #print("TEMP LOAD KEYS {}".format(temp_load.keys()))

                    print("json load took {:.2f} seconds".format(time.time() - begin_t))
                    # now get the values of the corresponding keys
                    found_docs[token] = found_token
                    print(found_docs)
                except KeyError:
                    print("key Error")
                    continue

        finally:
            for file in open_files.values():
                file.close()


        print("Searching took {:.2f}".format(time.time()-begin))
        print(tokenized_query)
        return tokenized_query

    @staticmethod
    def _read_json(file, to_find=None) -> {}:
        """
        Given a file to read from and a potential query, either seek the json file for the query or just return a
        read json file
        :param file: The file object to read from
        :param to_find: The potential query to search for
        :return: Dictionary containing either the whole read value from the file or a specific value to search for
        """

        if to_find:
            temp_holding = None
            for line in file:
                temp_holding = ujson.loads(line)
                if to_find in temp_holding.keys():
                    break
            file.seek(0)
            return temp_holding
        else:
            return json.load(file)

    @staticmethod
    def _clean_query(to_clean: [str]) -> [str]:
        """
        Given a query for search, remove stopwords and stem the queries for search ing
        :param to_clean: The list of values to clean
        :return: A list of cleaned values
        """
        cleaned = []
        stopword = set(stopwords.words('english'))
        dictionary = set(words.words())
        stemmer = PorterStemmer()
        for word in to_clean:
            stemmed = stemmer.stem(word)
            #print("word {} stemmed {}".format(word, stemmed))
            if word not in stopword and word in dictionary:
                cleaned.append(stemmed)
            elif word.isalnum() and len(word) < 10:
                cleaned.append(stemmed)

        #print("CLEANED {}".format(cleaned))
        return cleaned

    @staticmethod
    def _check_category(to_check: str) -> str:
        """
        Given a string, check its first letter and categorize it depening on its value
        :param to_check:
        :return:
        """
        begin = time.time()
        alphabet = [i for i in string.ascii_lowercase]
        alphabet = [set(i) for i in np.array_split(alphabet, 4)]
        alphabet.append(set(['1','2','3','4','5','6','7','8','9']))
        #print("TO CHECK {}".format(to_check[0]))

        to_return = ""
        if to_check[0] in alphabet[0]:
            to_return = 'atog'

        if to_check[0] in alphabet[1]:
            to_return = 'hton'

        if to_check[0] in alphabet[2]:
            to_return = 'otot'

        if to_check[0] in alphabet[3]:
            to_return = 'utoz'

        if to_check[0] in alphabet[4]:
           to_return = 'oneto9'

        print("Category checking took {:.2f} seconds.".format(time.time() - begin))
        #print("TO RETURN {}".format(to_return))
        return to_return

    @staticmethod
    def open_file(dir: str) -> {}:
        """
        Opens all the files within the given directory
        :param dir: The directory to open all the files from
        :return: A list of all the files that are opened
        """
        begin = time.time()
        open_file_list = {}
        try:
            for (root, dirs, files) in os.walk(dir):
                for file in files:
                    open_file_list[file.split('.')[0]] = open(root+file, 'r')

        except FileNotFoundError:
            print("Error, given directory {} is not a valid path. Please check again.".format(dir))
            for file in open_file_list:
                file.close()
                return {}
        print("File openning took {:.2f} seconds.".format(time.time() - begin))
        return open_file_list
