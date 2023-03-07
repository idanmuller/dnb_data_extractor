#Groups Handler
#the idea here is to save the search on groups by using file per group
#if file named as identification_number exsist the debt will be added to the file
#else new file named identification_number will be generated with current record debt data
#when using chunks is threads each group file will be per thread and will be merged in next step for output
import shutil
import sqlite3
import os
import json
import threading
from datetime import datetime
import constant
from extractor_instruction_data import ExtractorInstructionData

class GroupsHandler:
    def __init__(self ,groups_folder_path , extractor_instruction_data,repo_mode):
        self.extrct_inst_data = extractor_instruction_data
        self.groups_folder_path = None
        self.db_conn = None
        self.repo_mode = repo_mode

        if repo_mode == constant.USE_FILES:
            now = datetime.now()  # current date and time
            date_time = now.strftime("%m-%d-%Y-%H-%M-%S")
            self.groups_folder_path = groups_folder_path + date_time
            if not os.path.exists(self.groups_folder_path):
                os.makedirs(self.groups_folder_path, exist_ok=True)
        if repo_mode == constant.USE_DB:
            self.db_conn = sqlite3.connect('dataextractor.db')
            cur = self.db_conn.cursor()
            # create table if it doesn't exist
            cur.execute('''CREATE TABLE IF NOT EXISTS groups_records
                           (group_id TEXT PRIMARY KEY, group_data TEXT)''')
            self.db_conn.commit()

    def __del__(self):
        if self.repo_mode == constant.USE_DB:
            self.db_conn.close()
#Function read the groups folder files , sort it by name so that same group files will be processed in sequance
#each group of files will be merged to single output entry as defined
    def merge_files_for_output(self,extrct_inst_data):
        # iterate over sorted group files
        # Get a list of all files in the groups folder
        group_files = sorted(os.listdir(self.groups_folder_path))
        current_id = ""
        prev_id = ""
        num_of_grp_entities = 0
        output_string = ""

        for i, filename in enumerate(group_files):
            file_path = os.path.join(self.groups_folder_path, filename)
            with open(file_path, "r", encoding="utf-8") as f:
                identification_number = os.path.basename(file_path).split("_")[0]

                # print start of group
                current_id = identification_number
                if current_id != prev_id:
                    if i != 0:
                        output_string +="]}"
                        output_string +='\n'
                        #print("]}")
                    output_string += f'{{{extrct_inst_data.get_grouping_field()}: "{current_id}", "{extrct_inst_data.get_group_entity_name()}": ['
                    #print(f'{{{extrct_inst_data.get_grouping_field()}: "{current_id}", "{extrct_inst_data.get_group_entity_name()}": [')
                    prev_id = current_id
                # print debt records
                for j, line in enumerate(f):
                    num_of_grp_entities += 1
                    record = line.strip()
#                    if j == 0 and i != 0:
#                        print(" , ")
                    if j == 0:
                        output_string += f'"{record}", '
                        #print(f'"{record}"', end='')
                    else:
                        output_string += f'"{record}" '
                        #print(f',"{record}"', end='')
        # print end of last group
        if prev_id != "":
            output_string += "]}"
            #print("]}")
            print(output_string)
            output_string=""


    #prepare the subset of fields from parsed record for adding entry to group in Files mode
    def write_record_to_group_file(self, record,identification_number, subset_fields_list , group_entity_name):
        record_data_set = {}
        record_data_set = self.prepare_subset(record,subset_fields_list)
        #Save group per thread do avoid colissions btweeen threads
        thread_id = threading.get_ident()
        thread_file_path = f"{identification_number}_{thread_id}"

        file_path = os.path.join(self.groups_folder_path, thread_file_path)
        # if file exists, append debt record to it
        if os.path.exists(file_path):
            with open(file_path, "a" , encoding="utf-8") as f:
                f.write(f"{json.dumps(record_data_set)}\n")
        else:
            # create new file and write debt to it
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(f"{json.dumps(record_data_set)}\n")

#prepare the subset of fields for adding record to group in DB mode
    def write_record_to_group_db(self, record, identification_number, subset_fields_list):
        record_data_set = {}
        record_data_set = self.prepare_subset(record, subset_fields_list)

        # Insert or replace the data in the table
        cur = self.db_conn.cursor()
        # insert or update row in database
        cur.execute('''INSERT OR REPLACE INTO groups_records (identification_number, data)
                      VALUES (?, COALESCE((SELECT data || ?   FROM groups_records WHERE identification_number = ?), ?))''',
                  (identification_number, str(record_data_set), identification_number, str(record_data_set)))

        # Commit the changes and close the connection
        self.db_conn.commit()


    # prepare  a subset fields for new record in a group
    def prepare_subset(self, record, subset_fields_list):
        subset = {}
        for fld in subset_fields_list:
            subset[fld] = record[fld]
        return subset

#Merge files from threads if needed
    def handle_groups_files_for_output(self, extrct_inst_data):
        # get a list of all files in the folder
        group_files_list = os.listdir(self.groups_folder_path)
        # sort the list based on the file name to iterate and merge the groups generated from possibly several threads
        group_files_list.sort()

        # initialize variables to keep track of current and previous identification numbers
        current_id = None
        prev_id = None

        # Iterate over all files in the groups folder
        for file_name in group_files_list:
            # Extract the identification number from the file name
            identification_number = file_name.split("_")[0]
            # If the identification number has changed, print the previous group
            if current_id is not None and current_id != identification_number:
                print_group(prev_id, current_id, output_file)
                prev_id = current_id

        for filename in os.listdir(self.groups_folder_path):
            # get the identification number from the file name
            identification_number = filename.split("_")[0]
            # if this is the first file, set the current identification number
            if not current_id:
                current_id = identification_number

            # if the identification number has changed, print the current group and reset variables
            if identification_number != current_id:
                current_id = identification_number

            file_path = os.path.join(self.groups_folder_path, filename)
            with open(file_path,'r' , encoding="utf-8") as f:
                identification_number = filename.split(".")[0]
                group_items = []
                for line in f:
                    group_items.append(line)
                result = {extrct_inst_data.get_grouping_field():   identification_number, extrct_inst_data.get_group_entity_name(): group_items}
                print(json.dumps(result))

    def handle_groups_in_DB_for_output(self,extrct_inst_data):
        print('handle_groups_in_DB_for_output')
        cur = self.db_conn.cursor()
        cur.execute("SELECT *  FROM groups_records ;")
        rows = cur.fetchall()

        for group_item in rows:
            identification_number = group_item[0]
            data = group_item[1]
            result = {extrct_inst_data.set_grouping_field(): identification_number, extrct_inst_data.get_group_entity_name(): data}

    def purge_groups_data(self):
        shutil.rmtree(self.groups_folder_path, ignore_errors=True)

    def print_db(self):
        cur = self.db_conn.cursor()
        cur.execute("SELECT *  FROM groups_records ;")
        print(cur.fetchall())
