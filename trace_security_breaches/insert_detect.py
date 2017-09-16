# -*- coding: utf-8 -*-
__author__ = 'Zhen Qin'

import os
import time
import json
import sys
import re
import ConfigParser
import copy
import subprocess

def get_inserted_records_by_log(log_path, table_name):
    result = []
    pattern = r'^.* LOG:  statement: (?P<statement>.*)$'
    error_pattern = r'^.* ERROR:  (?P<error_reason>.*)$'
    flag = 0
    try:
        with open(log_path) as log_file:
            for line in log_file:
                if flag:
                    error_record = re.match(error_pattern, line)
                    if error_record:
                        result.pop(-1)
                        flag = 0
                        continue

                flag = 0
                sql = re.match(pattern, line)
                if sql:
                    sql_dict = sql.groupdict()
                    statement = sql_dict['statement'].strip()
                    if statement.lower().startswith('insert'):
                        if not statement.endswith(';'):
                            appending_lines = ''
                            while not appending_lines.endswith(';'):
                                appending_lines += log_file.next().strip()
                            statement += ' ' + appending_lines

                        parsed_statement = parsing_insert_statement(statement, table_name)
                        if parsed_statement:
                            result.append(parsed_statement)
                            flag = 1
    except IOError as e:
        print e
        sys.exit(-1)

    return result

def parsing_insert_statement(statement, table_name):
    pattern = r'^insert into (?P<table_name>.*?) (?P<columns>.*)values \((?P<column_values>.*)\);$'
    statement_info = re.match(pattern, statement, re.I).groupdict()
    if statement_info['table_name'].lower() == table_name.lower():
        columns = statement_info['columns'].strip()
        cols = []
        if columns:
            cols = [i.strip() for i in columns.strip('()').split(',')]
        col_vals = [i.strip().strip(''''"''') for i in statement_info['column_values'].strip().split(',')]

        return [cols, col_vals]

def get_inserted_records_by_real(dbfile_json_path):
    result = []
    a = []

    try:
        with open(dbfile_json_path) as data_file:
            for line in data_file:
                data = json.loads(line.strip())
                records = data['row_data']['records']
                for i in records:
                    print records[i]['status'], records[i]['data']
                    if records[i]['status']:
                        result.append(records[i]['data'])
                print '=='*70
    except IOError as e:
        print e
        sys.exit(-1)

    return result
    
def get_unmarked_inserted_records(need_dice=False):
    dbfile_json_path, log_path, table_name, columns, dbfile_name, dbfile_path, dbfile_new_path, dice_path = get_settings()

    if need_dice:
        run_dice(dbfile_name, dbfile_path, dbfile_new_path, dice_path)

    marked_insert_records = get_inserted_records_by_log(log_path, table_name)
    real_insert_records = get_inserted_records_by_real(dbfile_json_path)
    unaccounted_records = copy.deepcopy(real_insert_records)

    for ist in marked_insert_records:
        tmp_record = [None] * len(columns)
        if not ist[0]:
            tmp_record = ist[1]
        else:
            for i in range(len(ist[0])):
                idx = columns.index(ist[0][i])
                tmp_record[idx] = ist[1][i]

        try:
            unaccounted_records.remove(tmp_record)
        except ValueError as e:
            pass
              
    return unaccounted_records

def get_settings():
    try:
        config = ConfigParser.RawConfigParser()
        config.read('settings.cfg')
        dbfile_json_path = config.get('insert', 'dbfile_json_path')
        log_path = config.get('insert', 'log_path')
        table_name = config.get('insert', 'table_name')
        columns = map(lambda x:x.strip(), config.get('insert', 'columns').split(','))

        dbfile_name = config.get('insert', 'dbfile_name')
        dbfile_path = config.get('insert', 'dbfile_path')
        dbfile_new_path = config.get('insert', 'dbfile_new_path')
        dice_path = config.get('insert', 'dice_path')

        return dbfile_json_path, log_path, table_name, columns, dbfile_name, dbfile_path, dbfile_new_path, dice_path
    except ConfigParser.NoOptionError as e:
        print e
        sys.exit(-1)

def flush_cache():
    x = os.popen("sudo service postgresql stop")
    time.sleep(3)
    x = os.popen("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'")
    time.sleep(3)
    x = os.popen("sudo service postgresql start")
    time.sleep(3)   

def run_dice(dbfile_name, dbfile_path, dbfile_new_path, dice_path):
    flush_cache()
    os.system('sudo cp %s %s' % (os.path.join(dbfile_path, dbfile_name), dbfile_new_path))
    os.system('sudo chown zhen.zhen %s' %  os.path.join(dbfile_new_path, dbfile_name))
    os.chdir(os.path.dirname(dice_path))
    os.system('python %s' %  dice_path)


if __name__ == '__main__':
    for i in get_unmarked_inserted_records():
        print i
