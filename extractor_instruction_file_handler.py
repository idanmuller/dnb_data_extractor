import re
import PyPDF2
import io
from extractor_instruction_data import ExtractorInstructionData

# ExtractorInstructionFileHandler
#   reads the instructions file to retrieve process elements:
#   Flat file format for parsing
#   adopting data
#   grouping fld
#   group subset
class ExtractorInstructionFileHandler:
    def __init__(self, instructions_file):
        self.extrctr_inst_data  = ExtractorInstructionData()
        self.instructions_file = instructions_file
        pdf_data = io.BytesIO(self.instructions_file.read())
        #start to get instructions from pdf
        self.extrctr_inst_data.set_grouping_field(self.get_grouping_id_field_from_pdffile(pdf_data))
        self.extrctr_inst_data.set_fixed_length_fields(self.get_fixed_width_fields_from_pdffile(pdf_data))
        group_entity_name, group_subset_fields = self.extract_group_entity_and_group_subset_fields_from_pdf(pdf_data)
        self.extrctr_inst_data.set_group_entity_name(group_entity_name)
        self.extrctr_inst_data.set_group_subset_fields(group_subset_fields)

    def get_extrctr_inst_data(self):
        return self.extrctr_inst_data

    # retrives grouping_id field to be used
    def get_grouping_id_field_from_pdffile(self, pdf_dt):
        grouping_field = None
        # Create a PDF reader object
        reader = PyPDF2.PdfReader(pdf_dt)

        # Loop over the pages in the PDF
        for page_num in range(len(reader.pages)):
            # Get the current page
            page = reader.pages[page_num]

            # Extract the text from the page
            page_text = page.extract_text()

            match = re.search(r'-\s*(\w+)\s*–\s*this field is used for grouping', page_text)
            # Extract the grouping field name
            if match:
                grouping_field = match.group(1)
        # If the grouping ID field was not found, return None
        return grouping_field

    def get_group_subset_fields_from_pdffile(self):
        # TODO add logic here to get it from pdf
        self.group_subset_fields = {'entity_name', 'situation', 'debt_amount', 'information_date'}
        return self.group_subset_fields

    def get_adopt_from_pdffile(self):
        # add logic here to get it from pdf
        adopt = None
        return adopt

    def extract_group_entity_and_group_subset_fields_from_pdf(self, pdf_data):
        reader = PyPDF2.PdfReader(pdf_data)
        entity_name = None
        fields = []
        # Loop over the pages in the PDF
        for page_num in range(len(reader.pages)):
            # Get the current page
            page = reader.pages[page_num]

            # Extract the text from the page
            page_text = page.extract_text()
            # Search for the section containing the entity name and list of fields
            pattern = r"- (\w+) – it should be a list gathering all the other fields from all the grouped records:(.*?)(?:\n\n|\Z)"

            section_regex = re.compile(pattern, re.DOTALL)

            match = section_regex.search(page_text)
            # Extract the entity name and list of fields from the matched section
            if match:
                entity_name = match.group(1)
                # TODO refine this to get matching list
                #               fields = [field.strip() for field in match.group(2).split(",")]
                rp_subset_fields = ['entity_name', 'situation', 'debt_amount', 'information_date']
        return entity_name, rp_subset_fields

    def get_group_subset_fields_from_pdffile(self):
        subset_fields_list = {}
        pdf_data = io.BytesIO(self.instructions_file.read())
        reader = PyPDF2.PdfReader(pdf_data)

        # Loop over the pages in the PDF
        for page_num in range(len(reader.pages)):
            # Get the current page
            page = reader.pages[page_num]

            # Extract the text from the page
            page_text = page.extract_text()

            match = re.search(
                r'-\s*(\w+)\s*–\s*debts – it should be a list gathering all the other fields from all the grouped records',
                page_text)
            # Extract the group subset_fields
            if match:
                grouping_field = match.group(1)
        # If the grouping ID field was not found, return None
        return grouping_field

    def get_fixed_width_fields_from_pdffile(self, pdf_data):
        # Extract the entity mapping and field lengths table from the provided PDF file
        pdf_reader = PyPDF2.PdfReader(pdf_data)
        # Search for the starting string 'Use provided table to assign keys to values from “data.txt” in fixed width format'
        start_str = 'in fixed width format'
        start_page = -1
        start_index = None
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            page_text = page.extract_text()
            start_index = page_text.find(start_str)
            if start_index != -1:
                start_page = page_num
                # advance start index to after 1st header line  'length'
                start_str = 'length'
                start_index = page_text.find(start_str, start_index)
                break

        # If the starting string is not found, return None
        if (start_page < 0):
            return None

        # Search for the end of the table
        end_str = 'Adapting'
        end_page = -1
        end_index = None
        for page_num in range(start_page + 1, len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            page_text = page.extract_text()
            end_index = page_text.find(end_str)
            if end_index != -1:
                end_page = page_num
                break

        # If the end of the table is not found, return None
        if end_page is None:
            return None

        # Extract the table data from the pages
        fields = []
        for page_num in range(start_page, end_page + 1):
            page = pdf_reader.pages[page_num]
            page_text = page.extract_text()
            if page_num == start_page:
                page_text = page_text[start_index + len(start_str):]
            if page_num == end_page:
                page_text = page_text[:end_index]
            lines = page_text.split('\n')
            for line in lines:
                line = line.strip()
                if line:

                    parts = line.split()
                    if len(parts) == 2:
                        fields.append((parts[0], int(parts[1])))

        return fields
