"""
Description: This file contains the functions that do not fit into the 
    categories of data collection, scheduling and sql.
    
@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import fnmatch
import os

def remove_files(file_list):
    """
    Removes the files in the file list
    
    :param: file_list: list of the files that should be removed
    :type: file_list: list of strings
    
    :return: None
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
        
    :param: directory: the directory to look into
    :type: filepath
    
    :param: name_contains: returns filenames that contain name_contains
    :type: name_contains: string
    
    :return: ret_list: a list of the filenames that contain name_contains
    :type: ret_list: string
    """
    ret_list = []
    match_str = '*%s*' % name_contains
    for file in os.listdir(directory):
        if fnmatch.fnmatch(file, match_str):
            ret_list.append(os.path.join(directory, file))
    return ret_list


def create_directories(dir_list):
    """
    Create directories in the directory list
    
    :param: dir_list: list of the files that should be removed
    :type: dir_list: list of strings
    
    :return: None
    """
    for direc in dir_list:
        if not os.path.isdir(direc):
            os.mkdir(direc)
        else:    # Show an error #
            print("Directory, %s, already exists" % direc)
    return None