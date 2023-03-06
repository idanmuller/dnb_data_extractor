# D&B Data Extractor

## About / Synopsis

The Data extractor is a tool to migrate entries from a  flat file provided in zip folder


## Table of contents

> * [Title / Repository Name](#title--repository-name)
>   * [About / Synopsis](#about--synopsis)
>   * [Table of contents](#table-of-contents)
>   * [Installation](#installation)
>   * [Usage](#usage)
>   * [program design and steps](#program design and steps)
>   * [Code Content](#Code Content)

## Installation
In order to test the script please run the following commands:

`pip install PyPDF2`
`pip install pdfminer`
`pip install pdfminer.six`
`pip install configparser`


## Usage
**process.py script :**

expects file path of data_extractor.zip
this zip contains 3 files:
   1. data_extractor_processing.pdf - instructions related to the data file and process
   2. entity_mapping.tsv - mapping table to be used in processing entries
   3. data.txt - flat file records matching data_extractor_processing.pdf

**config file:** data_extractor_config.cfg
please refer to details below:
#repo_mode options:FILES/DB - use file as DB was added for POC and due to performances aspects file was working better 
repo_mode = FILES

#op_mode options:TEST/LIVE - use  LIVE for production ans QA or TEST if timing data should be added
op_mode = LIVE

#chunk_size 0 : dont split , else number of records in single chunk to parse
chunk_size = 1000
#max_threads high number will increase memory as parallel threads will hold entries of data 
max_threads = 20 
temp_folder_path = groups_runtime_folder - this is path to temporary folder where group files will be generated

## program design and steps

Process flow steps:
1. extract and read instructions from "data_extractor_processing.pdf"
   ExtractorInstructionFileHandler is the class handling this task

2. data file handling
    1. using the instructions to read(stream) the data entries adopt each entry using adaptor
    2. each entry will be added to group file
    3. to improve performance Handle_data_file will split the data file into chunks using chunk_size
       each chunk is handled in a thread using process_chunk function
   4. process_chunk function will read the chunk entries from data file ,adopt them and generate a group entry
   5. group entry will search the group file using the grouping_field,
   6. if file doesn't exist new file will be generated named with group id and thread - to avoid threads writing to each other files.
      example: "2N0HUR4OKON_21028" - "2N0HUR4OKON" is group id and "21028" is the unique thread id
   7. if file exist add the group entry to file.
   8. if any chunk handling fails - exception handling will report the chunk and last processed number so it can be tracked.
      this is done to **avoid proceeding** to next step that can conclude wrong entries in output 
   9. if all chunks/threads processed completes - move to merging the groups files and print to output
    
3. groups handling for output
logic merge_files_for_output  will read files in the groups folder in sorted by name
   1. logic merge_files_for_output  will read files in the groups folder in sorted by name
   2. all entries of same group from all threads will be printed under a single output entry - this is the merging logic

4. in LIVE mode purge the temporary folder

## Code Content/references
1. process.py  - Entry point / main
   it generates instance of DataExtractor and call its run function
2. DataExtractor.py - orchestrator of the process.
   run() function is the entry point: performing the steps:
      1. initiate instruction data using pdf file
      2. initiate the Adaptor
      3. initiate GroupsHandler  
      4. call Handle_data_file(...) to parse and group records
      5. process_chunk(...)  is called when chunks is defined to enable multi threading.
      6. call merge_files_for_output(...) to collect the groups and print the output
   
3. ExtractorInstructionData.py  - data struct holding information required by flow steps 
4. ExtractorInstructionData - performs logics to extract data from the instrction pdf into the ExtractorInstructionData
5. Adaptor - performs adaptation of data on record.
6. GroupsHandler - performs logics related to groups of entries
   1. write_record_to_group_file(...) - is called after record parsed
   2. merge_files_for_output(...) - takes care of the files of groups and prints to output



