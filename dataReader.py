import json
import os
from os import listdir
from os.path import isfile, join
import mysql.connector
from tkinter import *
import csv
import pandas as pd

'''
An external system / supplier is sending patient data to our platform using the FHIR standard. 
Our analytics teams find this format difficult to work with when creating dashboards and visualizations. 
This program will transform these FHIR messages into a more workable format.
'''


def connect_patient_db():
    """
    Function to connect to the patient database
    :return: the mySql connection or False if connection fails
    """
    try:
        return mysql.connector.connect(
            host="localhost",
            user="patient_db_admin",
            password="root",
            database="patient_database"
        )
    except:
        print("The Patient Database does not exist.")
        return False


def init_database():
    """
    Function to initialise the patient database
    If the database exists then it will return true
    If the database does not exist it will create the database patient_database on the mySQL server
    along with the appropriate tables patient, patient_identifier, patient_contact, patient_event and patient_language

    If the function cannot connect to the mySQL server it will return false

    :return: Boolean
    """
    # try to connect to the patient_database
    if not connect_patient_db():
        # if can't connect to patient_database as it does not exist then create it
        print("Creating Patient Database...")
        try:
            root_con = mysql.connector.connect(
                host="localhost",
                user="patient_db_admin",
                password="root"
            )

        except:
            print("Error: cannot connect to the server")
            return False

        root_cursor = root_con.cursor()

        # create patient_database
        root_cursor.execute("CREATE DATABASE patient_database")
        root_con.close()

        con = connect_patient_db()
        db_cursor = con.cursor()

        # create patient table
        db_cursor.execute(
            "CREATE TABLE patient (patient_id INT NOT NULL AUTO_INCREMENT, unique_id VARCHAR(37), "
            "given_name VARCHAR(255), "
            "family_name VARCHAR(255), birth_date VARCHAR(255), birth_sex VARCHAR(2), gender VARCHAR(255), "
            "mother VARCHAR(255), marital_status VARCHAR(255), name_use VARCHAR(255), address_line VARCHAR(255), "
            "address_city VARCHAR(255), address_state VARCHAR(255), address_country VARCHAR(255), address_latitude "
            "DECIMAL(8,6), address_longitude DECIMAL(9,6), birth_city VARCHAR(255), birth_state VARCHAR(255), "
            "birth_country VARCHAR(255), us_core_ethnicity VARCHAR(255), us_core_race VARCHAR(255), prefix VARCHAR("
            "255), death_dateTime VARCHAR(255), multiple_birth VARCHAR(255), disability_adjusted_life_years FLOAT, "
            "quality_adjusted_life_years FLOAT, PRIMARY KEY(patient_id))")

        # create patient_contact table
        db_cursor.execute(
            "CREATE TABLE patient_contact (patient_contact_id MEDIUMINT NOT NULL AUTO_INCREMENT, patient INT, "
            "contact_system VARCHAR(255), type VARCHAR(255), value VARCHAR(255), PRIMARY KEY(patient_contact_id), "
            "FOREIGN KEY (patient) REFERENCES patient(patient_id))")

        # create patient_identifier table
        db_cursor.execute(
            "CREATE TABLE patient_identifier (patient_identifier_id MEDIUMINT NOT NULL AUTO_INCREMENT, patient INT , "
            "id_system VARCHAR(255), type VARCHAR(255), value VARCHAR(255), PRIMARY KEY(patient_identifier_id), "
            "FOREIGN KEY (patient) REFERENCES "
            "patient(patient_id))")

        # create patient_language table
        db_cursor.execute(
            "CREATE TABLE patient_language (patient_language_id MEDIUMINT NOT NULL AUTO_INCREMENT, patient INT, "
            "language VARCHAR(255), PRIMARY KEY(patient_language_id), FOREIGN KEY (patient) REFERENCES "
            "patient(patient_id))")

        # create patient_event table
        db_cursor.execute(
            "CREATE TABLE patient_event (patient_event_id MEDIUMINT NOT NULL AUTO_INCREMENT, patient INT, "
            "event_data LONGTEXT, type VARCHAR(255), PRIMARY KEY(patient_event_id), FOREIGN KEY (patient) REFERENCES "
            "patient(patient_id))")

        con.close()
        print("Patient Database created successfully")
        print()
        return True
    else:
        return True


def add_patient(patient_data, patient_id):
    """
    A function to add the patient to the database and record their information
    :param patient_data: the json formatted data for the patient
    :param patient_id: the unique patient ID to be used when adding to the database
    :return:
    """

    con = connect_patient_db()
    db_cursor = con.cursor()

    birth_date = patient_data["birthDate"]
    birth_sex = patient_data["extension"][3]["valueCode"]
    mothers_maiden_name = str(patient_data["extension"][2]["valueString"]).replace("'", "''")
    marital_status = patient_data["maritalStatus"]["text"]
    gender = patient_data["gender"]
    family_name = str(patient_data["name"][0]["family"]).replace("'", "''")
    name_use = patient_data["name"][0]["use"]
    disability_adjusted_life_years = str(patient_data["extension"][5]["valueDecimal"])
    quality_adjusted_life_years = str(patient_data["extension"][6]["valueDecimal"])

    home_address_line = str(patient_data["address"][0]["line"][0]).replace("'", "''")
    home_address_city = str(patient_data["address"][0]["city"]).replace("'", "''")
    home_address_state = str(patient_data["address"][0]["state"]).replace("'", "''")
    home_address_country = str(patient_data["address"][0]["country"]).replace("'", "''")
    home_address_longitude = str(patient_data["address"][0]["extension"][0]["extension"][0]["valueDecimal"])
    home_address_latitude = str(patient_data["address"][0]["extension"][0]["extension"][1]["valueDecimal"])

    birth_place_city = str(patient_data["extension"][4]["valueAddress"]["city"]).replace("'", "''")
    birth_place_state = str(patient_data["extension"][4]["valueAddress"]["state"]).replace("'", "''")
    birth_place_country = str(patient_data["extension"][4]["valueAddress"]["country"]).replace("'", "''")

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
        multiple_birth = str(patient_data["multipleBirthBoolean"])
    except:
        multiple_birth = ""

    given_name = ""
    for name in patient_data["name"][0]["given"]:
        given_name += name + " "
    given_name = given_name[:-1].replace("'", "\'")

    try:
        sql = "INSERT INTO patient_database.patient (`patient_id`,`unique_id`,`given_name`,`family_name`," \
              "`birth_date`,`birth_sex`,`gender`,`mother`,`marital_status`,`name_use`,`address_line`," \
              "`address_city`,`address_state`,`address_country`,`address_latitude`,`address_longitude`," \
              "`birth_city`,`birth_state`,`birth_country`,`us_core_ethnicity`,`us_core_race`,`prefix`," \
              "`death_dateTime`,`multiple_birth`,`disability_adjusted_life_years`,`quality_adjusted_life_years`) " \
              "VALUES (NULL, '" + patient_id + "', '" + given_name + "', '" + family_name + "', '" + birth_date + \
              "', '" + birth_sex + "', '" + gender + "', '" + mothers_maiden_name + "', '" + marital_status + "', '" + name_use + "', '" + home_address_line + "', '" + home_address_city + "', '" + home_address_state + "','" + home_address_country + "', '" + home_address_latitude + "', '" + home_address_longitude + "', '" + birth_place_city + "', '" + birth_place_state + "', '" + birth_place_country + "', '" + us_core_ethnicity + "', '" + us_core_race + "', '" + prefix + "', '" + death_date_time + "', '" + multiple_birth + "', '" + disability_adjusted_life_years + "', '" + quality_adjusted_life_years + "') "
        db_cursor.execute(sql.rstrip("\n"))
        con.commit()
        db_patient_id = db_cursor.lastrowid
    except Exception as e:
        print()
        print("ERROR: Could not add " + given_name + " " + family_name)
        print()
        con.close()
        return None

    con.close()
    return db_patient_id


def add_patient_language(language, patient_id):
    """
    Function to add communication method/language for a patient
    A patient may have many languages or communication methods
    :param language: the language/communication method to be added
    :param patient_id: the unique patient ID for the patient in question
    :return:
    """

    con = connect_patient_db()
    db_cursor = con.cursor()
    try:
        sql = "INSERT INTO `patient_database`.`patient_language`(`patient_language_id`,`patient`,`language`) VALUES (NULL,'" + str(
            patient_id) + "','" + language + "');"
        db_cursor.execute(sql.rstrip("\n"))
        con.commit()
    except Exception as e:
        print("ERROR: Could not add language for" + patient_id)
        con.close()
        return False
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
        identifier_type = str(identifier["type"]["text"]).replace("'", "''")
    except:
        identifier_type = "-"
    identifier_system = identifier["system"]
    identifier_value = identifier["value"]
    con = connect_patient_db()
    db_cursor = con.cursor()
    try:
        sql = "INSERT INTO `patient_database`.`patient_identifier`(`patient_identifier_id`,`patient`,`id_system`,`type`,`value`)VALUES (NULL, '" + str(
            patient_id) + "','" + identifier_system + "', '" + identifier_type + "', '" + identifier_value + "');"
        db_cursor.execute(sql.rstrip("\n"))
        con.commit()
    except Exception as e:
        print("ERROR: Could not add identifier for" + patient_id)
        con.close()
        return False

    con.close()
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

    con = connect_patient_db()
    db_cursor = con.cursor()
    try:
        sql = "INSERT INTO `patient_database`.`patient_contact`(`patient_contact_id`,`patient`,`contact_system`,`type`,`value`)VALUES (NULL, '" + str(
            patient_id) + "','" + telecom_system + "', '" + telecom_use + "', '" + telecom_value + "');"
        db_cursor.execute(sql.rstrip("\n"))
        con.commit()
    except Exception as e:
        print("ERROR: Could not add identifier for" + patient_id)
        con.close()
        return False

    con.close()
    return True


def add_event(event_data, patient_id):
    """
    Function to add patient event data to the database.
    The data is stored as JSON data so it can be used later by the interface to create CSV files.
    :param event_data: the event data in json format
    :param patient_id: the unique id for the patient related to the event
    :return: 
    """

    con = connect_patient_db()
    db_cursor = con.cursor()
    try:
        formatted_event_data = json.dumps(event_data["resource"]).replace('"', '""').replace("'", "''")
        sql = "INSERT INTO `patient_database`.`patient_event`(`patient_event_id`,`patient`,`event_data`, `type`)VALUES (NULL, '" + str(
            patient_id) + "','" + formatted_event_data + "','" + event_data["resource"]["resourceType"] + "');"
        db_cursor.execute(sql.rstrip("\n"))
        con.commit()
    except Exception as e:
        print("ERROR: Could not add event for" + patient_id)
        con.close()
        return False

    con.close()
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

        con = connect_patient_db()
        db_cursor = con.cursor()
        db_cursor.execute("SELECT * FROM patient WHERE unique_id='" + unique_id + "'")
        patient = db_cursor.fetchall()

        # if the patient does not exist
        if len(patient) == 0:
            # Add the patient to the database
            db_patient_id = add_patient(patient_data, unique_id)

            # record patient languages in the database
            for language in patient_data["communication"]:
                add_patient_language(language["language"]["text"], db_patient_id)

            # record patient contact information in the database
            for telecom in patient_data["telecom"]:
                add_patient_contact(telecom, db_patient_id)

            # record patient identifiers in the database
            for identifier in patient_data["identifier"]:
                add_patient_identifier(identifier, db_patient_id)

            # record all patient events in the database
            for event in json_data["entry"]:
                if event["resource"]["resourceType"] != "Patient":
                    pass
                    #add_event(event, db_patient_id)
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


def create_csv_files():
    """
    A function to create formatted CSV files for each patient in the database
    and all corresponding events
    :return: True
    """
    print("Generating CSV files.....")
    # Check is csv folder exists if not create it
    if not os.path.isdir('csv'):
        os.mkdir('csv')

    # for each patient create a directory and fill it with events and patient csv files
    con = connect_patient_db()
    db_cursor = con.cursor()
    db_cursor.execute("SELECT * FROM patient")
    patients = db_cursor.fetchall()

    db_cursor.execute("SHOW COLUMNS FROM patient")
    headers = db_cursor.fetchall()
    header = []
    for item in headers:
        header.append(item[0])

    for patient in patients:
        folder_name = patient[2] + "_" + patient[3] + "_" + patient[1]
        if not os.path.isdir('csv/' + folder_name):
            os.mkdir('csv/' + folder_name)

        # Create patient file
        with open('csv/' + folder_name + '/patient.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerow(patient)

        db_cursor.execute("SELECT * FROM patient_event WHERE patient=" + str(patient[0]))
        events = db_cursor.fetchall()

        for event in events:
            # Create event file
            formatted_event_data = str(event[2]).replace("''","'").replace('""','"').replace('<div xmlns="http://www.w3.org/1999/xhtml">',"<div>").strip("\n")
            df = pd.read_json(formatted_event_data, lines=True, encoding='utf-8-sig')
            df.to_csv('csv/' + folder_name + '/event_' + str(event[0]) + '_' + str(event[3]) + '.csv', index=None)

    con.close()
    print("CSV files generated successfully")
    print("They are now available in the csv folder")

    return True


def load_app_interface():
    """
    A function to display the contents of the database to the user and allow them to generate CSV files
    at the click of a button.

    Currently incomplete and not in use.

    :return:
    """
    app = Tk()
    app.title("Medical Records")
    app.geometry("900x600")
    app.resizable(width=0, height=0)
    app['bg'] = 'white'

    frame = Frame(app)
    frame.pack(expand=True, fill=BOTH)  # .grid(row=0,column=0)
    canvas = Canvas(frame, bg='#FFFFFF', width=900, height=600)
    hbar = Scrollbar(frame, orient=HORIZONTAL)
    hbar.pack(side=BOTTOM, fill=X)
    hbar.config(command=canvas.xview)
    vbar = Scrollbar(frame, orient=VERTICAL)
    vbar.pack(side=RIGHT, fill=Y)
    vbar.config(command=canvas.yview)
    canvas.config(xscrollcommand=hbar.set, yscrollcommand=vbar.set)

    con = connect_patient_db()
    db_cursor = con.cursor()
    db_cursor.execute("SELECT given_name, family_name, unique_id FROM patient")
    patients = db_cursor.fetchall()
    con.close()

    # this wil create a label widget
    l1 = Label(canvas, text="Unique ID", width=37)
    l2 = Label(canvas, text="Given Name", width=18)
    l3 = Label(canvas, text="Family Name", width=18)
    l4 = Label(canvas, text="Events", width=18)
    l5 = Label(canvas, text="Profile CSV", width=20)
    l6 = Label(canvas, text="Events CSV", width=10)

    # grid method to arrange labels in respective
    # rows and columns as specified
    l1.grid(row=0, column=1, sticky=W, pady=2)
    l2.grid(row=0, column=2, sticky=W, pady=2)
    l3.grid(row=0, column=3, sticky=W, pady=2)
    l4.grid(row=0, column=4, sticky=W, pady=2)
    l5.grid(row=0, column=5, sticky=W, pady=2)
    l6.grid(row=0, column=6, sticky=W, pady=2)

    i = 1
    for patient in patients:
        l1 = Label(canvas, text=patient[2], width=37)
        l2 = Label(canvas, text=patient[0], width=18)
        l3 = Label(canvas, text=patient[1], width=18)
        l4 = Label(canvas, text="Events", width=18)
        l5 = Label(canvas, text="Profile CSV", width=20)
        l6 = Label(canvas, text="Events CSV", width=10)

        l1.grid(row=i, column=1, sticky=W, pady=1)
        l2.grid(row=i, column=2, sticky=W, pady=1)
        l3.grid(row=i, column=3, sticky=W, pady=1)
        l4.grid(row=i, column=4, sticky=W, pady=1)
        l5.grid(row=i, column=5, sticky=W, pady=1)
        l6.grid(row=i, column=6, sticky=W, pady=1)
        i += 1

    canvas.configure(scrollregion=canvas.bbox("all"))
    canvas.pack(side=LEFT, expand=True, fill=BOTH)

    app.mainloop()


if __name__ == '__main__':
    # check if database exists or create if it doesn't exist
    if init_database():
        # load data files and process to fill database
        load_json_files("data")

        # create csv files
        create_csv_files()

        # load interface
        # load_app_interface()
