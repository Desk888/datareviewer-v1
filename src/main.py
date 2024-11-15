from app import open_files
from app import check_missing_values

def main():
    check_missing_values(open_files())

if __name__ == '__main__':
    main()