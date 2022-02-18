import json
from os import listdir
from os.path import isfile, join

'''
An external system / supplier is sending patient data to our platform using the FHIR standard. 
Our analytics teams find this format difficult to work with when creating dashboards and visualizations. 
This program will transform these FHIR messages into a more workable format.
'''


def process_json(json_data, file_path):
    """"
    :param file_path: the path for the file the json data came from
    :param json_data: a Json object that was created by load_json_data
    :return: True if handled without error, False if there is an error
    """
    if json_data["entry"][0]["resource"]["resourceType"] == "Patient":
        patient_data = json_data["entry"][0]["resource"]
        unique_id = patient_data["id"]
        birth_date = patient_data["birthDate"]
        marital_status = patient_data["maritalStatus"]["text"]
        gender = patient_data["gender"]
        family_name = patient_data["name"][0]["family"]
        name_use = patient_data["name"][0]["use"]

        try:
            prefix = patient_data["name"][0]["prefix"][0]
        except:
            prefix = ""

        try:
            death_date_time = patient_data["deceasedDateTime"]
        except:
            death_date_time = "n/a"

        try:
            multiple_birth = patient_data["multipleBirthBoolean"]
        except:
            multiple_birth = ""

        given_name = ""
        for name in patient_data["name"][0]["given"]:
            given_name += name + " "
        given_name = given_name[:-1]

        telecom_info = []
        for telecom in patient_data["telecom"]:
            telecom_system = telecom["system"]
            telecom_use = telecom["use"]
            telecom_value = telecom["value"]
            telecom_info.append({"system":telecom_system, "use":telecom_use, "value":telecom_value})

        identifiers = []
        for identifier in patient_data["identifier"]:
            try:
                identifier_type = identifier["type"]["text"]
            except:
                identifier_type = "-"
            identifier_system = identifier["system"]
            identifier_value = identifier["value"]
            identifiers.append({"system":identifier_system, "type":identifier_type, "value":identifier_value})

        print("ID: " + unique_id)
        print("Name: " + prefix + " " + given_name + " " + family_name)
        print("DOB: " + birth_date)
        print("DOD: " + death_date_time)
        print("Gender: " + gender)
        print("Marital Status: " + marital_status)
        print("Multiple Birth: " + str(multiple_birth))
        print("Contact Information")
        for contact in telecom_info:
            print("--- "+contact["system"]+" | "+contact["use"]+" | "+contact["value"])
        print("Identifiers")
        for id in identifiers:
            print("--- " + id["system"] + " | " + id["type"] + " | " + id["value"])
        print()
    else:
        print("ERROR: Patient Details not found for " + file_path)

    return True


def load_json_data(file_path):
    """
    Function to load the JSON data from a given JSON file and create a json Object with it's data
    :param file_path: A file path to a .json file
    :return: True if successful, False if unable to load
    """
    with open(file_path, 'r') as file:
        try:
            if process_json(json.loads(file.read()), file_path):
                return True
            else:
                print("ERROR: Cannot process " + file_path)
                print()
                return False
        except:
            print("ERROR: " + file_path + " is in the wrong format")
            print()
            return False


def load_json_files(directory):
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
                if load_json_data(directory + "/" + f):
                    loaded_count += 1
            else:
                print(f + " is not a .json file")
    print()
    print("Loaded " + str(loaded_count) + " Successfully")
    return True


if __name__ == '__main__':
    load_json_files("data")
