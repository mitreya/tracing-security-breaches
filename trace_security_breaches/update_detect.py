# -*- coding: utf-8 -*-
__author__ = 'Zhen Qin'

import re
import copy
import ConfigParser

from insert_detect import get_unmarked_inserted_records
from delete_detect import get_unmarked_deleted_records

def get_updated_records_by_log(log_path, table_name):
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
                    if statement.lower().startswith('update'):
                        if not statement.endswith(';'):
                            appending_lines = ''
                            while not appending_lines.endswith(';'):
                                appending_lines += log_file.next().strip()
                            statement += ' ' + appending_lines

                        parsed_statement = parsing_update_statement(statement, table_name)
                        if parsed_statement:
                            result.append(parsed_statement)
                            flag = 1
    except IOError as e:
        print e
        sys.exit(-1)

    return result

def parsing_update_statement(statement, table_name):
    pattern = r'^update (?P<table_name>.*?) set (?P<new_values>.*);$'
    if ' where ' in statement.lower():
        pattern = r'^update (?P<table_name>.*?) set (?P<new_values>.*) where (?P<conditions>.*);$'
    statement_info = re.match(pattern, statement, re.I).groupdict()
    if statement_info['table_name'].lower() == table_name.lower():
        conds = statement_info.get('conditions', [])
        if conds:
            conds = [i.strip() for i in conds.strip().split('=')]
        new_values = statement_info['new_values'].strip()
        nvs = [i.strip().split('=') for i in new_values.split(',')]

        return [nvs, conds]

def get_unmarked_updated_records():
    dbfile_json_path, log_path, table_name, columns, dbfile_name, dbfile_path, dbfile_new_path, dice_path = get_settings()

    #run_dice(dbfile_name, dbfile_path, dbfile_new_path, dice_path)

    unmarked_inserted_records = get_unmarked_inserted_records()
    unmarked_deleted_records = get_unmarked_deleted_records()
    marked_update_records = get_updated_records_by_log(log_path, table_name)

    for delete_record in unmarked_deleted_records:
        for update_record in marked_update_records:
            if update_record[1]:
                if delete_record[columns.index(update_record[1][0])] == update_record[1][1]:
                    unmarked_deleted_records.remove(delete_record)
                    tmp_updated_info = copy.deepcopy(delete_record)
                    for i in update_record[0]:
                        tmp_updated_info[columns.index(i[0])] = i[1].strip().strip(''''"''')
                    for insert_record in unmarked_inserted_records:
                        if tmp_updated_info == insert_record:
                            unmarked_inserted_records.remove(insert_record)

    return unmarked_deleted_records, unmarked_inserted_records

def get_settings():
    try:
        config = ConfigParser.RawConfigParser()
        config.read('settings.cfg')
        dbfile_json_path = config.get('update', 'dbfile_json_path')
        log_path = config.get('update', 'log_path')
        table_name = config.get('update', 'table_name')
        columns = map(lambda x:x.strip(), config.get('update', 'columns').split(','))

        dbfile_name = config.get('update', 'dbfile_name')
        dbfile_path = config.get('update', 'dbfile_path')
        dbfile_new_path = config.get('update', 'dbfile_new_path')
        dice_path = config.get('update', 'dice_path')

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
    for i in get_unmarked_updated_records():
        print i
