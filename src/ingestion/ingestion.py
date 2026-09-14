from pyarrow import fs
from datetime import date , timedelta
import subprocess
import sys
from  pathlib import Path
RUN_DATE = date.today() - timedelta(days=3)
BASE_PATH= '/user/linux'
BRONZE_PATH='/user/linux/bronze'
STG_PATH='/user/linux/stg_temp_write'
FILE_NAME = 'access_logs.txt'
PARENT_DIR = f'date={RUN_DATE}'


def get_source_file_path():
    current_dir = Path.cwd()
    print("cur path:" , current_dir)
    project_root = current_dir.parent.parent
    return project_root / "source_system" / f"{FILE_NAME}"


def is_file_from_source_exists():

    
    # Connect to the HDFS cluster
    hdfs = fs.HadoopFileSystem('localhost', port=9000 , user='linux')

    # list bronze to check the date= directory
    dir_selector = fs.FileSelector(BRONZE_PATH, recursive=False)

    #check if parent directory exists
    parent_dir_exists = False
    for info in hdfs.get_file_info(dir_selector):
        if str(info.base_name).strip().upper() == PARENT_DIR.strip().upper():
            parent_dir_exists = True
            break
    

    # get the run date and construct the path
    log_file_path = f'{BRONZE_PATH}/date={RUN_DATE}'
        
    if parent_dir_exists:
        file_exists= False
        file_selector = fs.FileSelector(log_file_path, recursive=False)
        for info in hdfs.get_file_info(file_selector):
            
            if str(info.base_name).strip().upper() ==  FILE_NAME.strip().upper():
                file_exists = True
                break

        
        return file_exists

    return False        
    


def run_ingestion():

    

        

        if is_file_from_source_exists():
            print("This file already exists. , Pipeline will stop execution!")
        else:


            try:
                result = subprocess.run(['hdfs','dfs','-rm' , '-r',STG_PATH] , check=True , capture_output=True)
            except subprocess.CalledProcessError:
                print(f"ERROR: Can't Delete {STG_PATH}")
                sys.exit(1)
            try:
                result = subprocess.run(['hdfs','dfs','-mkdir','-p',STG_PATH],check=True , capture_output=True)
            except subprocess.CalledProcessError:
                print(f"ERROR: Can't Create th {STG_PATH}")
                sys.exit(1)

            try:
                #copy to temp
                copy_command = ['hdfs' , 'dfs' , '-put', get_source_file_path(), STG_PATH]
                result = subprocess.run(copy_command,check=True , capture_output=True)
            except subprocess.CalledProcessError:
                print(f"ERROR: Can't Copy the Data File from the system")
                sys.exit(1)
            

            try:
                # create directory with date into the bronze layer
                create_dir_command = ['hdfs', 'dfs' , '-mkdir','-p' , f'{BRONZE_PATH}/{PARENT_DIR}']
                result = subprocess.run(create_dir_command , check=True , capture_output=True)
            except subprocess.CalledProcessError:
                print(f"ERROR: Can't Create this Directory {PARENT_DIR}")
                sys.exit(1)

            # Renmae / Change the pointer 
            try:
                rename_command =  ['hdfs' , 'dfs','-mv', f'{STG_PATH}/access_logs.txt' , f'{BRONZE_PATH}/{PARENT_DIR}/{FILE_NAME}']
                result = subprocess.run(rename_command, capture_output=True , check=True)
            except subprocess.CalledProcessError:
                print("ERROR: Can't Rename the file to target path")
                sys.exit(1)



            print("File Loaded Successfully!")            



if __name__ == '__main__':
    run_ingestion()
   
    
