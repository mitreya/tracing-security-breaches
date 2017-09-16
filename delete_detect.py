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

def get_deleted_records_by_log(log_path, table_name):
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
                    if statement.lower().startswith('delete'):
                        if not statement.endswith(';'):
                            appending_lines = ''
                            while not appending_lines.endswith(';'):
                                appending_lines += log_file.next().strip()
                            statement += ' ' + appending_lines

                        parsed_statement = parsing_delete_statement(statement, table_name)
                        if parsed_statement:
                            result.append(parsed_statement)
                            flag = 1
    except IOError as e:
        print e
        sys.exit(-1)

    return result

def parsing_rule_1(condition):
    result = [i.strip().strip('"').strip("'") for i in condition.split('=')]
    result.append('=')

    return result

def parsing_rule_2(condition):
    result = [i.strip().strip('"').strip("'") for i in condition.split(' like ')]
    result.append('like')

    return result

def parsing_rules_chain(condition):
    rules = [parsing_rule_1, parsing_rule_2]
    for r in rules:
        result = r(condition.lower())
        if result and len(result) == 3:
            return result

def parsing_delete_statement(statement, table_name):
    pattern = r'^delete from (?P<table_name>.*) where (?P<condition>.*);$'
    statement_info = re.match(pattern, statement, re.I).groupdict()
    result = []
    if statement_info['table_name'].lower() == table_name.lower():
        conditions = map(lambda x: x.strip(), re.split(' and ', statement_info['condition']))
        for cond in conditions:
            result.append(parsing_rules_chain(cond))

    return result
    #conditions = map(lambda x: x.strip(), re.split('and|or', statement.lower().split('where')[1].strip().strip(';')))
    #for i in conditions:
    #    result.append(parsing_rule_1(i))

def get_deleted_records_by_real(dbfile_json_path):
    result = []

    try:
        with open(dbfile_json_path) as data_file:
            for line in data_file:
                data = json.loads(line.strip())
                records = data['row_data']['records']
                for i in records:
                    print records[i]['status'], records[i]['data']
                    if not records[i]['status']:
                        result.append(records[i]['data'])
                print '=='*70
    except IOError as e:
        print e
        sys.exit(-1)

    return result
    
def get_unmarked_deleted_records(need_dice=False):
    '''parameter need_dice is used to determine if the program needs to run DICE 
       and get the latest data from databases before executing all the logics.
       The default value is False.
    '''
    dbfile_json_path, log_path, table_name, columns, dbfile_name, dbfile_path, dbfile_new_path, dice_path = get_settings()

    if need_dice:
        run_dice(dbfile_name, dbfile_path, dbfile_new_path, dice_path)

    marked_del_records = get_deleted_records_by_log(log_path, table_name)
    real_del_records = get_deleted_records_by_real(dbfile_json_path)
    unaccounted_records = copy.deepcopy(real_del_records)

    for conds in marked_del_records:
        for dr in real_del_records:
            try:
                del_flag = 0
                for cond in conds:
                    idx = columns.index(cond[0])
                    if cond[-1] == '=' and dr[idx] == cond[1]:
                        del_flag = 1
                    elif cond[-1] == 'like' and '%' in cond[1]:
                        tmp_cond = cond[1]
                        pattern = tmp_cond.replace('%', '(.*?)')
                        if re.match(pattern, dr[idx]):
                            del_flag = 1
                    else:
                        del_flag = 0

                if del_flag:
                    unaccounted_records.remove(dr)

            except ValueError as e:
                pass
 
    return unaccounted_records

def get_settings():
    try:
        config = ConfigParser.RawConfigParser()
        config.read('settings.cfg')
        dbfile_json_path = config.get('delete', 'dbfile_json_path')
        log_path = config.get('delete', 'log_path')
        table_name = config.get('delete', 'table_name')
        columns = map(lambda x:x.strip(), config.get('delete', 'columns').split(','))

        dbfile_name = config.get('delete', 'dbfile_name')
        dbfile_path = config.get('delete', 'dbfile_path')
        dbfile_new_path = config.get('delete', 'dbfile_new_path')
        dice_path = config.get('delete', 'dice_path')

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
    for i in get_unmarked_deleted_records():
        print i
