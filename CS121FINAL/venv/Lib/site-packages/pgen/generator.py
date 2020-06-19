#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# PGen
# Copyright (C) 2014-2015  Vladislav Belov (Kazan Federal University)
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import os
import random
import subprocess
import sys
import json


class Generator:
    """
        Data Generator
    """

    def __init__(self, data_description, html_report=False, debug_mode=False):
        self.data_description = {}
        if isinstance(data_description, str):
            self.data_description = json.loads(data_description)
        else:
            self.data_description = data_description
        self.count = self.data_description['count']
        self.format = self.data_description['format']
        self.output = self.data_description['output']

        self.problem_input = data_description
        self.html_report = html_report
        self.html_report_tests = ''
        self.problem_path = None
        self.tests_path = None

        self.current_test_variables = {}
        self.current_variables = {}
        self.debug_mode = debug_mode

        self.seed = None
        if 'seed' in data_description:
            self.seed = data_description['seed']
        random.seed(self.seed)

    def write(self):
        data_items = []

        # generate data
        for data_index in range(self.count):
            self.current_variables = {}
            data_items.append(self.generate_type(self.format))

        # output data
        if self.output['type'] == 'stdout':
            for data_item in data_items:
                print(self.to_string(self.format, data_item))
        elif self.output['type'] == 'file':
            index = 0
            if 'start_index' in self.output:
                index = int(self.output['start_index'])
            data_path = os.path.dirname(self.output['format'].format(index))

            # create dir for data if not present
            if not os.path.isdir(data_path):
                try:
                    os.makedirs(data_path)
                except OSError:
                    # TODO: use Python3 os.makedirs(path, exist_ok=True)
                    pass

            for data_item in data_items:
                data_file_handle = open(self.output['format'].format(index), 'w')
                data_file_handle.write(self.to_string(self.format, data_item))
                data_file_handle.close()
                index += 1
        else:
            # TODO: throw exception
            pass

    def get_html_report_tests(self):
        return self.html_report_tests

    def generate_test(self, to_string=None, validator=None):
        self.current_test_variables = {}
        ans = []
        for i in range(0, len(self.problem_input)):
            val = self.generate_type(self.problem_input[i], ans)
            ans.append(val)
        if validator is not None:
            data = ''
            if to_string is not None:
                data = to_string(ans)
            else:
                for i in range(0, len(self.problem_input)):
                    data += self.to_string(self.problem_input[i], ans[i])
            cnt = 0
            while not validator(ans, data):
                cnt += 1
                self.current_test_variables = {}
                ans = []
                for i in range(0, len(self.problem_input)):
                    val = self.generate_type(self.problem_input[i], ans)
                    ans.append(val)
                data = ''
                if to_string is not None:
                    data = to_string(ans)
                else:
                    for i in range(0, len(self.problem_input)):
                        data += self.to_string(self.problem_input[i], ans[i])

        if to_string is not None:
            test_data = to_string(ans)
        else:
            test_data = ''
            for i in range(0, len(self.problem_input)):
                test_data += self.to_string(self.problem_input[i], ans[i])
        return test_data

    def generate(self, problem_path, test_count, manual_tests=[], solution=None, to_string=None,
                 after_test_callback=None, validation=None, data_format='%03d.dat', answer_format='%03d.ans'):
        self.problem_path = os.path.abspath(problem_path)
        self.tests_path = os.path.join(self.problem_path, 'tests')
        if not os.path.isdir(self.problem_path):
            os.mkdir(self.problem_path)
        if not os.path.isdir(self.tests_path):
            os.mkdir(self.tests_path)

        path_format = os.path.join(self.tests_path, data_format)
        path_ans_format = os.path.join(self.tests_path, answer_format)
        problems_dir = os.path.dirname(self.problem_path)

        html_report_content = ''

        # make solution
        solution_path = os.path.join(self.problem_path, 'solutions', 'solution.cpp')
        run_path = os.path.join(self.problem_path, 'solutions', 'solution')
        if solution is None:
            if os.path.isfile(solution_path):
                p = subprocess.Popen(
                    ['g++', '-O2', solution_path, '-o', run_path],
                    stdout=subprocess.PIPE,
                    stdin=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                p.communicate()
            else:
                pass

        # add random test
        tests = manual_tests
        for i in range(0, test_count - len(manual_tests)):
            self.current_test_variables = {}
            ans = []
            for i in range(0, len(self.problem_input)):
                val = self.generate_type(self.problem_input[i], ans)
                ans.append(val)
            if validation is not None:
                data = ''
                if to_string is not None:
                    data = to_string(ans)
                else:
                    for i in range(0, len(self.problem_input)):
                        data += self.to_string(self.problem_input[i], ans[i])
                cnt = 0
                while not validation(ans, data):
                    cnt += 1
                    self.current_test_variables = {}
                    ans = []
                    for i in range(0, len(self.problem_input)):
                        val = self.generate_type(self.problem_input[i], ans)
                        ans.append(val)
                    data = ''
                    if to_string is not None:
                        data = to_string(ans)
                    else:
                        for i in range(0, len(self.problem_input)):
                            data += self.to_string(self.problem_input[i], ans[i])
            if after_test_callback is not None:
                ans = after_test_callback(ans)
            tests.append(ans)

        # output tests
        if tests:
            for key, test in enumerate(tests):
                key += 1
                html_report_content += '<tr class="line%d"><td class="number">%s</td>' % (key % 2, key,)
                # test data
                path = path_format % key
                dat = open(path, 'w')
                data = ''
                if to_string is not None:
                    data = to_string(test)
                else:
                    for i in range(0, len(self.problem_input)):
                        data += self.to_string(self.problem_input[i], test[i])
                dat.write(data)
                dat.close()
                print(path.replace(problems_dir, ''))

                # TODO: data to visual data
                if self.debug_mode:
                    print('Test data:')
                    print(data)

                if len(data) > 300:
                    html_report_content += '<td>%s...</td>' % (data[:300].replace('\n', '<br>'),)
                else:
                    html_report_content += '<td>%s</td>' % (data.replace('\n', '<br>'),)

                # ans
                path = path_ans_format % key

                solution_data = ''
                if solution is not None:
                    ans = open(path, 'w')
                    print(path.replace(problems_dir, ''))
                    solution_data = str(solution(test))
                    ans.write(solution_data)
                    ans.close()
                else:
                    if os.path.isfile(run_path):
                        ans = open(path, 'w')
                        print(path.replace(problems_dir, ''))
                        p = subprocess.Popen([run_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
                        stdout_data = p.communicate(input=data)[0]
                        solution_data = stdout_data
                        ans.write(stdout_data)
                        ans.close()
                    else:
                        # TODO: if no solution
                        pass

                if self.debug_mode:
                    print('Solution data:')
                    print(solution_data)

                if len(solution_data) > 300:
                    html_report_content += '<td>%s...</td>' % (solution_data[:300].replace('\n', '<br>'), )
                else:
                    html_report_content += '<td>%s</td>' % (solution_data.replace('\n', '<br>'), )

                html_report_content += '</tr>'

        if self.html_report:
            html_report_content_template_handle = open(os.path.join(os.path.dirname(__file__), 'report_template.html'),
                                                       'r')
            html_report_content_template = html_report_content_template_handle.read()
            html_report_content_template_handle.close()

            html_report_content_table_template_handle = open(
                os.path.join(os.path.dirname(__file__), 'report_table_template.html'), 'r')
            html_report_content_table_template = html_report_content_table_template_handle.read()
            html_report_content_table_template_handle.close()

            self.html_report_tests = html_report_content_table_template.replace('###LINES###', html_report_content)
            self.html_report_tests = self.html_report_tests.replace('###NAME###', os.path.basename(self.problem_path))
            # html_report_content = html_report_content_template.replace('###LINES###', html_report_content)
            html_report_content = html_report_content_template.replace('###TABLE###', self.html_report_tests)
            html_report_content = html_report_content.replace('###TITLE###', os.path.basename(self.problem_path))

            html_report_handle = open(os.path.join(self.problem_path, 'report.html'), 'w')
            html_report_handle.write(html_report_content)
            html_report_handle.close()


    def generate_manual(self, manual_tests, to_string, manual_answers=None, solution=None, after_test_callback=None,
                        validation=None, data_format='%03d.dat', answer_format='%03d.ans'):
        path_format = os.path.join(self.tests_path, data_format)
        path_ans_format = os.path.join(self.tests_path, answer_format)
        problems_dir = os.path.dirname(self.problem_path)

        tests = manual_tests

        # output tests
        if tests:
            for key, test in enumerate(tests):
                key += 1
                # dat
                path = path_format % key
                dat = open(path, 'w')
                data = to_string(test)
                dat.write(data)
                dat.close()
                print(path.replace(problems_dir, ''))
                solution_data = ''
                if manual_answers is not None:
                    solution_data = manual_answers[key - 1]
                else:
                    if solution is not None:
                        solution_data = str(solution(test))
                    else:
                        solution_path = os.path.join(self.problem_path, 'solutions', 'solution.cpp')
                        run_path = os.path.join(self.problem_path, 'solutions', 'solution')

                        # make solution
                        p = subprocess.Popen(['g++', '-O2', solution_path, '-o', run_path], stdout=subprocess.PIPE,
                                             stdin=subprocess.PIPE, stderr=subprocess.PIPE)
                        p.communicate()

                        p = subprocess.Popen([run_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
                        stdout_data = p.communicate(input=data)[0]
                        solution_data = stdout_data
                # ans
                if solution_data:
                    path = path_ans_format % key
                    ans = open(path, 'w')
                    print(path.replace(problems_dir, ''))
                    ans.write(solution_data)
                    ans.close()

    def generate_type(self, element, test=None):
        result = None
        # int
        if element['type'] == 'int':
            result = self.generate_int(element)
        # double
        if element['type'] == 'double':
            result = self.generate_double(element)
        # pair
        if element['type'] == 'pair':
            result = [self.generate_type(element['first']), self.generate_type(element['second'])]
        # container
        if element['type'] == 'container':
            result = [self.generate_type(item) for item in element['elements']]
        # point
        if element['type'] == 'point':
            result = self.generate_point(element)
        # segment
        if element['type'] == 'segment':
            result = self.generate_segment(element)
        # string
        if element['type'] == 'string':
            if element['length'] == 'random':
                element['length'] = random.randint(element['from'], element['to'])
            if str(element['length']) == 'depend':
                element['length'] = test[element['depends']['length']]
            if 'patterns' not in element:
                element['patterns'] = None
            if 'special_chars' not in element:
                element['special_chars'] = None
            result = self.generate_string(element['length'], element['patterns'], element['special_chars'])
        # array
        if element['type'] == 'array':
            length = element['length']
            if str(length) == 'depend':
                length = test[element['depends']['length']]
            result = self.generate_array(element, length, element['subelement'])
        # tree
        if element['type'] == 'tree':
            result = self.generate_tree(element, test)
        # graph
        if element['type'] == 'graph':
            result = self.generate_graph(element)
        # add variable value to list
        if 'name' in element and result is not None:
            self.current_variables[element['name']] = result
        return result

    def generate_string(self, length, patterns=None, special_chars=None):
        # make alphabet from pattern
        if special_chars is None:
            special_chars = ''
        alphabet = special_chars
        if patterns is None:
            patterns = ['A-Z']
        for pattern in patterns:
            pattern = pattern.split('-')
            if len(pattern) != 2:
                continue
            for i in range(ord(pattern[0]), ord(pattern[1]) + 1):
                alphabet += chr(i)
        if isinstance(length, str):
            length = self.current_variables[length]
        return ''.join([random.choice(alphabet) for i in range(0, length)])

    def generate_int(self, element):
        _from = element['from']
        _to = element['to']
        if isinstance(element['from'], str):
            _from = self.current_test_variables[element['from']]
        if isinstance(element['to'], str):
            _to = self.current_test_variables[element['to']]
        value = self.random_int(_from, _to)
        if 'id' in element:
            self.current_test_variables[element['id']] = value
        return value

    def generate_double(self, element):
        value = self.random_double(element['from'], element['to'])
        if 'id' in element:
            self.current_test_variables[element['id']] = value
        return value

    def generate_array(self, element, length, subelement):
        if isinstance(element['length'], str):
            length = self.current_variables[element['length']]
        return [self.generate_type(subelement) for i in range(0, length)]

    def generate_permutation(self, length):
        print('legacy permutation')
        ls = [i for i in range(1, length + 1)]
        for i in range(0, random.randint(1, length / 2)):
            p = random.randint(0, length - 1)
            l = random.randint(1, min(p, length - p))
            ls = ls[:p - l] + ls[p - l:p + l] + ls[p + l:]
        return ls

    def generate_tree(self, element, test):
        # TODO: replace to check Id exist
        size = 0
        if True:
            size = self.current_test_variables[element['size']]
        else:
            size = random.randint(element['from'], element['to'])
        # generate
        if size < 1:
            return []
        if size == 1:
            if 'representation' in element:
                if element['representation'] == 'matrix':
                    return [[0]]
                else:
                    return []
            else:
                return []
        vertex = [i for i in range(0, size)]
        vertex_used = []
        edges = []
        while len(vertex) > 0:
            vertex_from = -1
            vertex_to = -1
            # select to vertex
            a = self.random_int(0, len(vertex) - 1)
            vertex_from = vertex[a]
            vertex = vertex[0:a] + vertex[a + 1:]
            if len(vertex_used) == 0:
                b = self.random_int(0, len(vertex) - 1)
                vertex_to = vertex[b]
                vertex = vertex[0:b] + vertex[b + 1:]
                vertex_used.append(vertex_to)
            else:
                b = self.random_int(0, len(vertex_used) - 1)
                vertex_to = vertex_used[b]
            vertex_used.append(vertex_from)
            edge = {
                'from': vertex_from,
                'to': vertex_to,
            }
            if 'edge' in element:
                if 'value' in element['edge']:
                    edge['value'] = self.generate_type(element['edge']['value'], test)
            edges.append(edge)

        representation = 'edgelist'
        if 'representation' in element:
            representation = element['representation']
        if representation == 'matrix':
            matrix = [[0 for i in range(0, size)] for j in range(0, size)]
            for edge in edges:
                i = edge['from']
                j = edge['to']
                value = 1
                if 'value' in edge:
                    value = edge['value']
                matrix[i][j] = value
                matrix[j][i] = value
            return matrix
        else:
            return edges

    def generate_graph(self, element):
        # TODO: make graph
        return []

    def generate_point(self):
        # TODO: to be implemented
        return 'NONE'

    def generate_triangle(self):
        # TODO: to be implemented
        return 'NONE'

    def generate_segment(self):
        # TODO: to be implemented
        return 'NONE'

    def to_string(self, element, value, is_last=False):
        if 'between' not in element:
            element['separator'] = ' '
        result = ''
        # int
        if element['type'] == 'int':
            if 'format' not in element:
                element['format'] = '%d'
            result = element['format'] % value
        # double
        if element['type'] == 'double':
            if 'format' not in element:
                element['format'] = '%.3f'
            result = element['format'] % value
        # pair
        if element['type'] == 'pair':
            result = self.to_string(element['first'], value[0], False) + element['between'] + self.to_string(
                element['second'], value[1], False)
        # container
        if element['type'] == 'container':
            if 'separator' not in element:
                element['separator'] = ' '
            for key, item in enumerate(element['elements']):
                if result:
                    result += element['separator']
                result += self.to_string(item, value[key], False)
        # string
        if element['type'] == 'string':
            if 'format' not in element:
                element['format'] = '%s'
            result = element['format'] % value
        # array
        if element['type'] == 'array':
            for item in value:
                if result:
                    result += element['separator']
                result += self.to_string(element['subelement'], item, False)
        if element['type'] == 'tree':
            if element['representation'] == 'matrix':
                for line in value:
                    result += ' '.join([str(item) for item in line]) + '\n'
                return result
            else:
                for edge in value:
                    val = ''
                    if 'value' in edge:
                        val = edge['value']
                    result += str(edge['from'] + ' ' + edge['to']) + val + '\n'
                return result
        if 'after' in element:
            result += element['after']
        return result

    @staticmethod
    def random_int(_from, _to):
        if _from >= _to:
            return _from
        else:
            return random.randint(_from, _to)

    @staticmethod
    def random_double(_from, _to, _epsilon=0.1):
        if _from >= _to:
            return _from, _epsilon
        else:
            return _from + random.random() * (_to - _from)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(0)
    mode = 'single'
    problem = {}
    seed = None
    for i in range(1, len(sys.argv)):
        data = sys.argv[i].split('=')
        param = data[0]
        value = None
        if len(data) > 1:
            value = data[1]
        if param == '-mode':
            if value == 'single':
                mode = 'single'
            if value == 'multiply':
                mode = 'multiply'
        if param == '-j':
            try:
                if not value:
                    value = 'problem.json'
                handle = open('problem.json', 'r')
                problem = json.loads(handle.read())
                handle.close()
            except ValueError:
                print('Incorrect problem format: %s' % value)
        if param == '-seed':
            seed = int(value)

    # TODO: make more modes
    gen = Generator(problem['input_format'], seed)
    if mode == 'single':
        print(gen.generate_test())