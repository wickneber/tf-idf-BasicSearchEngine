
import bs4

"""
Extracts given html file into different parts
"""


class HtmlExtractor:
    def __init__(self, to_extract=None):
        """
        Initializes the HtmlExtractor to extract desired tags
        :param to_extract: a list of the tags to extract
        """
        if to_extract is None:
            to_extract = []
        self._to_extract = to_extract  # a list of tags to extract

    def extract_tags(self, to_parse: str, verbose=False) -> (str, str):
        """
        Given an html file to parse into its weighted parts and its rest
        :param to_parse: The html file to parse
        :param verbose: A boolean value determining if we want this function to be verbose for debugging purposes
        :return: a tuple containing a string of the weighted values and a string containing the rest of the content
        """
        extracted_tags = []

        # get a base bs4 instance for extracting the weighted tags
        html_to_extract = bs4.BeautifulSoup(to_parse, 'html.parser')

        # iterate through each weighted tag
        for tag in self._to_extract:
            # Find all instance of the tag
            discovered_tags = html_to_extract.find_all(tag)

            # iterate through the discovered tags
            for discovered in discovered_tags:
                # add the found instance into the extracted tags list
                extracted_tags.append(discovered)
                # now, subtract the found instance from the bs4 instance
                discovered.extract()

        # extract any tags that are outright unwanted
        for unwanted_tag in html_to_extract(["script", "style"]):
            unwanted_tag.extract()

        # now, we want to change the extracted tag type from bs4.element.ResultSet to a list of string
        # temp_tags is a temporary placeholder for the extracted strings
        extracted_tags = HtmlExtractor._bs4_to_list(extracted_tags)

        if verbose:
            print("Desired tags:\n{}\n".format(extracted_tags))
            print("Remaining tags: {}".format(html_to_extract))

        # now join the the list of found tags into one big string to make tokenizing easier
        extracted_tags = HtmlExtractor._join_list(extracted_tags)
        joined_html = HtmlExtractor._join_list(html_to_extract.get_text().splitlines())

        return extracted_tags, joined_html

    @staticmethod
    def _join_list(to_join: [str], delimeter=" ") -> str:
        """
        Given a list of str, join the list into one large string
        :param to_join: The list of str to join
        :param delimeter: The value to place between each joint
        :return: the resulting string after joining
        """
        temp_join = delimeter
        return temp_join.join(to_join)

    @staticmethod
    def _bs4_to_list(to_list: [bs4.element.ResultSet]) -> [str]:
        """
        Just gets a bs4.element.ResultSet instance and turns it into a list of string
        :param to_list: bs4.element.ResultSet instance to change
        :return: A list of str
        """
        temp_tags = []
        for tag in to_list:
            temp_tags.append(tag.get_text())
        return temp_tags
