import csv
import decimal
from datetime import datetime, timedelta
import sys

# Adaptor
class Adaptor:
    def __init__(self, entity_file):
        self.entity_mapping = {}
        self.parse_mapping_file(entity_file)

    # handling the ',' as decimal point:
    def number_figures_handler(self, num_str):
        new_num_str = num_str.replace(",", ".")
        return decimal.Decimal(new_num_str)

    def calculate_debt_amount(self, record):
        loans = self.number_figures_handler(record["loans"])
        participations = self.number_figures_handler(record["participations"])
        guarantees_granted = self.number_figures_handler(record["guarantees_granted"])
        other_concepts = self.number_figures_handler(record["other_concepts"])
        debt_amount = "{:.2f}".format(1000 * (loans + participations + guarantees_granted + other_concepts))
        return debt_amount

    def parse_mapping_file(self, entity_file):
        entity_reader = csv.reader(entity_file, delimiter='\t')
        # Create a dictionary to store the entity mappings
        # Iterate over the rows in the entity mapping file and add them to the dictionary
        next(entity_reader)  # skip header
        for row in entity_reader:
            self.entity_mapping[row[0]] = row[1]

    # Adapt a single record per to the rules
    def adopt_sngl_record(self, record):
        # Only keep valid situation codes and transform code "11" to "1"

        situation = record["situation"]
        if situation == "11":
            situation = "1"
        elif situation < "1" or situation > "6":
            # record is discarded
            sys.stderr.write('invalid situation in record - identification_number=' + record["identification_number"])
            print('invalid situation in record - identification_number=' + record["identification_number"])
            return None
        record["situation"] = situation
        # Map entity_code to entity_name
        entity_code = record["entity_code"]
        record["entity_name"] = self.entity_mapping.get(entity_code)

        # Parse information_date and convert to ISO format
        information_date = record["information_date"]
        year = int(information_date[:4])
        month = int(information_date[4:])
        last_day_of_month = (datetime(year, month, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        record["information_date"] = last_day_of_month.date().isoformat()

        # Compute debt_amount as specified
        record["debt_amount"] = self.calculate_debt_amount(record)

        return record
