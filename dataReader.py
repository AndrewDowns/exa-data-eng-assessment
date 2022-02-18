import json
from os import listdir
from os.path import isfile, join

'''
An external system / supplier is sending patient data to our platform using the FHIR standard. 
Our analytics teams find this format difficult to work with when creating dashboards and visualizations. 
This program will transform these FHIR messages into a more workable format.
'''


def handle_json(file_path):
    """
    Function to load the JSON data from a given JSON file and create a json Object with it's data
    :param file_path: A file path to a .json file
    :return: True if successful, False if unable to load
    """
    with open(file_path, 'r') as file:
        try:
            data = json.loads(file.read())
            return True
        except:
            print("ERROR: " + file_path + " is in the wrong format")
            return False


def load_data(directory):
    """
    Function to load in any JSON files in the given starting directory.
    :param directory: A directory containing files in JSON Format
    :return: True if directory exists, False if it does not or does not contain files.
    """
    print(str(len(listdir(directory))) + " Files found in " + directory)
    print("Loading....")
    print()
    loaded_count = 0
    for f in listdir(directory):
        if isfile(join(directory, f)):
            if f[-5:] == ".json":
                if handle_json(directory + "/" + f):
                    loaded_count += 1
            else:
                print(f+" is not a .json file")
    print()
    print("Loaded " + str(loaded_count) + " Successfully")
    return True


if __name__ == '__main__':
    load_data("data")
