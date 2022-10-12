"""
Description: This file contains the functions that do not fit into the 
    categories of data collection, scheduling and sql.
    
@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import fnmatch
import os
import pandas as pd


def remove_files(file_list):
    """
    Removes the files in the file list
    
    :param file_list: list of the files that should be removed
    :type file_list: list of strings
    
    :return None
    """
    for file in file_list:
        if os.path.isfile(file):
            os.remove(file)
        else:    # Show an error #
            print("Error: %s file not found" % file)
    return None


def find_files_that_filename_contain(directory, name_contains):
    """
    Return a list of files in a directory where the filename contains 
        name_contains
        
    :param directory: the directory to look into
    :type file path
    
    :param name_contains: returns file names that contain name_contains
    :type name_contains: string
    
    :return ret_list: a list of the file names that contain name_contains
    :type ret_list: string
    """
    ret_list = []
    match_str = '*%s*' % name_contains
    for file in os.listdir(directory):
        if fnmatch.fnmatch(file, match_str):
            ret_list.append(os.path.join(directory, file))
    return ret_list


def create_directories(directory_list):
    """
    Create directories in the directory list
    
    :param directory_list: list of the files that should be removed
    :type directory_list: list of strings
    
    :return None
    """
    for directory in directory_list:
        if not os.path.isdir(directory):
            os.mkdir(directory)
        else:    # Show an error #
            print("Directory, %s, already exists" % directory)
    return None


def convert_xls_to_dict_file(xls_file, out_file_name):
    """
    This converts the sheets in an xls file into a dict that is written to a
        *.py file. This file will be imported later.

    :param xls_file: file path to the xls file. Each row will be an entry and
        each column is a key for that entry.
    :type xls_file: string

    :param out_file_name: filename for the output file
    :type string

    :return None
    """
    xls_data = pd.read_excel(xls_file, sheet_name=None)
    # create a list of the sheet names. This will be the variable names
    sheet_names = [x for x in xls_data.keys()]
    # write the dictionaries to files
    with open(out_file_name, 'w') as outfile:
        for sInd in range(len(sheet_names)):
            sheet_name = sheet_names[sInd]
            # dict like {index -> {column -> value}} and convert to string
            dict_str = str(xls_data[sheet_name].to_dict('index'))
            # add new lines to make it more readable
            dict_str = dict_str.replace('}, ', '},\n')
            outfile.write('%s = { \n' % sheet_name)
            # remove the beginning { because added to line above
            outfile.write(dict_str[1:])
            # add a new line between the dictionaries
            if sInd < len(sheet_names) - 1:
                outfile.write('\n')
