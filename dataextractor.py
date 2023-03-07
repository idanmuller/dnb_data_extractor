import psutil
import concurrent
import os
import sys
from concurrent import futures
import constant
import configparser
import io
import zipfile
from datetime import datetime
import PyPDF2
from groupshandler import GroupsHandler
from adaptor import Adaptor
from extractor_instruction_file_handler import ExtractorInstructionFileHandler
from extractor_instruction_data import ExtractorInstructionData


# logic: stream line by line
# each line parsed using fields map
# using adaptor to adapt the record
# calling grphander to group record into runtime files

class DataExtractor:
    def __init__(self, file_path):
        self.filepath = file_path

    # handles single line - parse + adopt
    def parse_and_Adapt_single_line(self, line, adaptor, extrct_inst_data):
        parsed_line = {}
        start = 0

        for field, length in extrct_inst_data.get_fixed_length_fields():
            end = start + length
            parsed_line[field] = line[start:end].strip()
            start = end
        # using adapter on record:
        adopted_record = adaptor.adopt_sngl_record(parsed_line)
        return adopted_record

    # handles chunk  - segment of the file
    #parse line => atopt => group
    def process_chunk(self, file_path, start, num_records, record_size, adaptor, extrct_inst_data, grphander,
                      repo_mode):
        try:
            chunk_processed_entries = 0
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                #        with zip_file.open('data.txt') as fixed_width_file:
                with io.TextIOWrapper(zip_file.open("data.txt"), encoding="utf-8") as fixed_width_file:

                    # Process each line in the chunk
                    # Skip to the starting line
                    fixed_width_file.seek(start * record_size)

                    # Process each record in the chunk
                    for i in range(num_records):
                        # Read the next record from the file
                        record = fixed_width_file.read(record_size)

                        # Parse the record and do something with it
                        # For example, print the record number and data
                        if len(record.strip()) == 0:
                            break
                        else:
                            adopted_record = self.parse_and_Adapt_single_line(record, adaptor, extrct_inst_data)
                            if adopted_record is not None:

                                # using group handler on adopted record:
                                if repo_mode == constant.USE_FILES:
                                    grphander.write_record_to_group_file(adopted_record,
                                                                         adopted_record[
                                                                             extrct_inst_data.grouping_field],
                                                                         extrct_inst_data.get_group_subset_fields(),
                                                                         extrct_inst_data.get_group_entity_name())
                                #added for POC on DB
                                if repo_mode == constant.USE_DB:
                                    # TODO re-orgenize list of subset in generic way
                                    subset_fields_list = {'entity_name', 'situation', 'debt_amount', 'information_date'}
                                    grphander.write_record_to_group_db(adopted_record,
                                                                       adopted_record['identification_number'],
                                                                       subset_fields_list)

                            chunk_processed_entries += 1
        except Exception as e:
            raise Exception(f"Error processing chunk start={start} ,num_records={num_records} , chunk_processed_entries={chunk_processed_entries} : {e}")

    def Handle_data_file(self, file_path, adaptor, grphander, extrct_inst_data, repo_mode, chunk_size,max_threads):
 #       print(file_path)
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            #        with zip_file.open('data.txt') as fixed_width_file:
            with io.TextIOWrapper(zip_file.open("data.txt"), encoding="utf-8") as fixed_width_file:

                # dont use chunks single run on data file
                if (chunk_size == 0):
                    for line in fixed_width_file:
                        adopted_record = self.parse_and_Adapt_single_line(line, adaptor, extrct_inst_data)
                        if adopted_record is not None:
                            # using group handler on write_record_to_group_file to opted record:
                            if repo_mode == constant.USE_FILES:
                                grphander.write_record_to_group_file(adopted_record,
                                                                     adopted_record[extrct_inst_data.grouping_field],
                                                                     extrct_inst_data.get_group_subset_fields(),
                                                                     extrct_inst_data.get_group_entity_name())
                            #used for POC only
                            if repo_mode == constant.USE_DB:
                                # re-orgenize list of subset in generic way
                                subset_fields_list = {'entity_name', 'situation', 'debt_amount', 'information_date'}
                                grphander.write_record_to_group_db(adopted_record,
                                                                   adopted_record['identification_number'],
                                                                   subset_fields_list)
                # use chunks with threads
                else:
                    # chunks logic
                    file_size = zip_file.getinfo("data.txt").file_size
                    # Read the first line to get the record size
                    line_size = len(fixed_width_file.readline())
                    # Seek back to the starting position
                    total_lines = file_size // line_size
                    # Calculate the number of chunks to process and split the work accordingly
                    chunks = [(i * chunk_size, chunk_size) for i in range(total_lines // chunk_size + 1)]

                    # Create and start the worker threads
                    #limit the number of threads to avoid memory issues
                    #TODO adjsut max_workers to memory size
                    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
                        futures = []
                        for start, size in chunks:
                            futures.append(
                                executor.submit(self.process_chunk, file_path, start, size, line_size, adaptor,
                                                extrct_inst_data, grphander, repo_mode))

                        # Wait for all the threads to finish
                        # in any thread failed stop the whole process to avoid printing wrong data
                        for future in concurrent.futures.as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                print(f"An error occurred: {e}")
                                sys.exit(1)

    # util function for config
    def get_config_dict(self):
        config = configparser.ConfigParser()
        config.read_file(open(r'data_extractor_config.cfg'))
        details_dict = dict(config.items('OP_SECTION'))
        return details_dict

    def run(self):
        config_details = self.get_config_dict()
        temp_folder = config_details['temp_folder_path']
        repo_mode = config_details['repo_mode']
        op_mode = config_details['op_mode']
        chunk_size = int(config_details['chunk_size'])
        max_threads = int(config_details['max_threads'])

        # debug  time test
        if op_mode == constant.TEST_MODE:
            start_dt = datetime.now()
            print('<<<START>>>')
            print(start_dt)
        # new instance
        extrct_inst_data = ExtractorInstructionData()

        # Read entity mapping file
        # Open the data_extractor.zip file
        with zipfile.ZipFile(self.filepath, 'r') as zip_file:
            with io.TextIOWrapper(zip_file.open('entity_mapping.tsv'), encoding="utf-8") as entity_file:
                adaptor = Adaptor(entity_file)

                # here we let ExtractorInstructionFileHandler get the data from pdf
                with zip_file.open('data_extractor_processing.pdf', 'r') as pdf_file:
                    extractor_inst_handler = ExtractorInstructionFileHandler(pdf_file)
                    extrct_inst_data = extractor_inst_handler.get_extrctr_inst_data()

                #handle the data file
                grphander = GroupsHandler(temp_folder, extrct_inst_data, repo_mode)
                self.Handle_data_file(self.filepath, adaptor, grphander, extrct_inst_data, repo_mode, chunk_size,max_threads)

                # debug  time test
                if op_mode == constant.TEST_MODE:
                    post_Handle_data_file_dt = datetime.now()
                    duration = post_Handle_data_file_dt - start_dt

                #Final step to use the groups files and merge it to the output
                if repo_mode == constant.USE_FILES:
                    #this is the actual logic to handle the outputs
                    grphander.merge_files_for_output(extrct_inst_data)
                if repo_mode == constant.USE_DB:
                    grphander.handle_groups_in_DB_for_output(extrct_inst_data)

        if op_mode == constant.TEST_MODE:
            end_dt = datetime.now()
            print('post_Handle_data_file_dt duration=', duration)
            print('<<<END>>>')
            print(end_dt)
            duration = end_dt - start_dt
            print('start 2 end duration=', duration)

#clear the disc from temp data files of groups
        if op_mode == constant.LIVE_MODE:
            grphander.purge_groups_data()

