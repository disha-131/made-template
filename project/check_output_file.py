import os

def check_output_file(expected_output_file):
    if os.path.isfile(expected_output_file):
        print(f"\t[SUCCESS] Output file {expected_output_file} found.")
    else:
        print(f"\t[ERROR] Unable to locate the expected output file: {expected_output_file}.")
        return

current_directory = os.path.dirname(os.path.abspath(__file__))
file_name = "../data/items_db.db"
file_path = os.path.join(current_directory, file_name)

if __name__ == "__main__":
    check_output_file(file_path)