"""
Description: This file contains the functions that do not fit into the 
    categories of data collection, scheduling and sql.
    
@author: Robert Hennessy (robertghennessy@gmail.com)
"""


import os

def remove_files(file_list):
    """
    Removes the files in the file list
    
    :param: file_list: list of the files that should be removed
    :type: list of strings
    
    :return: None
    """
    for file in file_list:
        if os.path.isfile(file):
            os.remove(file)
        else:    # Show an error #
            print("Error: %s file not found" % file)