import sys

from dataextractor import DataExtractor

def main(file_path):
    extractor =  DataExtractor(file_path)
    extractor.run()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please provide file path as argument")
#TODO refer to arg only
        file_path = 'data_extractor.zip'
        main(file_path)

    else:
        file_path = sys.argv[1]
        main(file_path)
