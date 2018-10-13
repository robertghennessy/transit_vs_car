# -*- coding: utf-8 -*-
"""
Created on Sun Sep 16 14:47:47 2018

@author: Robert
"""

import json
import numpy as np
import os
import pandas as pd

import config

tst_list = [('date', 'text', 1), ('utc_time', 'real', 3), ('time', 'text', 2)]

table_def = {
    1: {'sql_name': 'date', 'pandas_name': 'pd_date', 'sql_type':'text'},
    3: {'sql_name': 'utc_time', 'pandas_name': 'pd_utc_time', 'sql_type':'real'},
    2: {'sql_name': 'time', 'pandas_name': 'pd_time', 'sql_type':'text'}}
            
keys = sorted(table_def.keys())

# create the table command
table_cmd = ''
for key in keys:
    if table_cmd != '':
        table_cmd = table_cmd + ', '
    table_cmd  = (table_cmd + table_def[key]['sql_name'] + ' ' + 
            table_def[key]['sql_type'])    
    
# create the command to reorder the database
# frame = frame[['column I want first', 'column I want second'...etc.]]

pandas_col_list = []
pandas_rename_dict = {}
for key in keys:
    pandas_col_list.append(table_def[key]['pandas_name'])
    pandas_rename_dict[table_def[key]['pandas_name']] = (table_def[key]
                                                            ['sql_name'])

# rename the columns
# df.rename(index=str, columns={"A": "a", "B": "c"})


def convert_xls_to_dict_file(xls_file, outFileName):
    """    
    This converts the sheets in an xls file into a dict that is written to a
        *.py file. This file will be imported later.
    
    :param: xls_file: file path to the xls file. Each row will be an entry and
        each column is a key for that entry.
    :type: xls_file: string
    
    :param: outFileName: filename for the output file
    :type: string
    
    :return: None
    """
    xls_data = pd.read_excel(xls_file,sheet_name=None)
    # create a list of the sheet names. This will be the variable names
    sheet_names = [x for x in xls_data.keys()]
    # write the dictionaries to files
    with open(outFileName, 'w') as outfile:
        for sInd in range(len(sheet_names)):
            sheet_name = sheet_names[sInd]
            # dict like {index -> {column -> value}} and convert to string
            dict_str  = str(xls_data[sheet_name].to_dict('index'))
            # add new lines to make it more readable
            dict_str = dict_str.replace('}, ',  '},\n')
            outfile.write('%s = { \n' % sheet_name)
            # remove the beginning { becuase added to line above
            outfile.write(dict_str[1:])
            # add a new line between the dictionaries
            if sInd < len(sheet_names) - 1:
                outfile.write('\n')

xls_file = os.path.join(config.base_dir,'sql_table_definition.xlsx')
convert_xls_to_dict_file(xls_file, 'test_table_def.py')

import test_table_def

def create_table_def_string(input_dict):
    """
    This creates a string that will be used for sql table definition.
    
    :param: input_dict: a dictionary that contains the sql_name and the 
        sql_type
    :type: input_dict: dictionary
    
    :return: ret_str:  
    """
    key_list = list(input_dict.keys())
    ret_str = ''
    for kInd in range(len(key_list)):
        key = key_list[kInd]
        ret_str = ret_str + '%s %s' % (input_dict[key]['sql_name'],
                                       input_dict[key]['sql_type'])
        if kInd < len(key_list) - 1:
                ret_str = ret_str + ', '
    return ret_str

tst = create_table_def_string(test_table_def.siri_dict)
#print(tst)

# check that column exists in the data frame
# reorded the columns
# select the columns that would be outputted

# 



ordered_list = ['A','B','C','G']
col_list = ['B','C','A','D','E']
xor_list = [a for a in ordered_list+col_list if (a not in ordered_list) or (a not in col_list)]
missing_list = [a for a in ordered_list if (a not in col_list)]

df = pd.DataFrame(np.random.randint(low=0, high=10, size=(5, 5)),
                  columns=col_list)
# below reorder the columns and if the column does not exist, it fills it 
# with NaN
df2 = df.reindex(columns=ordered_list)

# creates the list to reindex the dataframe
input_dict = test_table_def.siri_dict
ordered_list = []
for key in range(len(input_dict)):
    ordered_list.append(input_dict[key]['pandas_name'])

df2 = df.reindex(columns=ordered_list)
