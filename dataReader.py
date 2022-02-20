import json
from os import listdir
from os.path import isfile, join

'''
An external system / supplier is sending patient data to our platform using the FHIR standard. 
Our analytics teams find this format difficult to work with when creating dashboards and visualizations. 
This program will transform these FHIR messages into a more workable format.
'''

def add_patient(patient_data, patient_id):
    """
    A function to add the patient to the database and record their information
    :param patient_data: the json formatted data for the patient
    :param patient_id: the unique patient ID to be used when adding to the database
    :return:
    """
    birth_date = patient_data["birthDate"]
    birth_sex = patient_data["extension"][3]["valueCode"]
    mothers_maiden_name = patient_data["extension"][2]["valueString"]
    marital_status = patient_data["maritalStatus"]["text"]
    gender = patient_data["gender"]
    family_name = patient_data["name"][0]["family"]
    name_use = patient_data["name"][0]["use"]
    disability_adjusted_life_years = patient_data["extension"][5]["valueDecimal"]
    quality_adjusted_life_years = patient_data["extension"][6]["valueDecimal"]

    home_address_line = patient_data["address"][0]["line"][0]
    home_address_city = patient_data["address"][0]["city"]
    home_address_state = patient_data["address"][0]["state"]
    home_address_country = patient_data["address"][0]["country"]
    home_address_longitude = patient_data["address"][0]["extension"][0]["extension"][0]["valueDecimal"]
    home_address_latitude = patient_data["address"][0]["extension"][0]["extension"][1]["valueDecimal"]

    birth_place_city = patient_data["extension"][4]["valueAddress"]["city"]
    birth_place_state = patient_data["extension"][4]["valueAddress"]["state"]
    birth_place_country = patient_data["extension"][4]["valueAddress"]["country"]

    us_core_ethnicity = patient_data["extension"][1]["extension"][0]["valueCoding"]["display"]
    us_core_race = patient_data["extension"][0]["extension"][0]["valueCoding"]["display"]

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

    return True


def add_patient_language(language, patient_id):
    """
    Function to add communication method/language for a patient
    A patient may have many languages or communication methods
    :param language: the language/communication method to be added
    :param patient_id: the unique patient ID for the patient in question
    :return:
    """
    return True


def add_patient_identifier(identifier, patient_id):
    """
    Function to add a form of identification for a patient
    A patient may have many forms of identification
    :param identifier: the particular identifier to be added
    :param patient_id: the unique patient id for the patient in question
    :return:
    """
    try:
        identifier_type = identifier["type"]["text"]
    except:
        identifier_type = "-"
    identifier_system = identifier["system"]
    identifier_value = identifier["value"]
    return True


def add_patient_contact(contact_info, patient_id):
    """
    Function to add patient contact information to the database
    A single patient could have many methods of contact
    :param contact_info: the contact information to be added
    :param patient_id: the unique patient id to be used for reference
    :return: True if successful, False if unsuccessful
    """
    telecom_system = contact_info["system"]
    telecom_use = contact_info["use"]
    telecom_value = contact_info["value"]
    return True


def add_event(event_data, patient_id):
    """
    Function to add patient event data to the database.
    The data is stored as JSON data so it can be used later by the interface to create CSV files.
    :param event_data: the event data in json format
    :param patient_id: the unique id for the patient related to the event
    :return: 
    """
    return True


def process_json(json_data, file_path):
    """"
    :param file_path: the path for the file the json data came from
    :param json_data: a Json object that was created by load_json_data
    :return: True if handled without error, False if there is an error
    """
    if json_data["entry"][0]["resource"]["resourceType"] == "Patient":
        patient_data = json_data["entry"][0]["resource"]
        unique_id = patient_data["id"]

        # Add the patient to the database
        add_patient(patient_data, unique_id)

        # record patient languages in the database
        for language in patient_data["communication"]:
            add_patient_language(language["language"]["text"], unique_id)

        # record patient contact information in the database
        for telecom in patient_data["telecom"]:
            add_patient_contact(telecom, unique_id)

        # record patient identifiers in the database
        for identifier in patient_data["identifier"]:
            add_patient_identifier(identifier, unique_id)

        # record all patient events in the database
        for event in json_data["entry"]:
            if event["resource"]["resourceType"] != "Patient":
                add_event(event, unique_id)

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
    # check if database exists or create if it doesn't exist

    # load data files and process to fill database
    load_json_files("data")

    # load interface
