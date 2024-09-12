import os
from pathlib import Path
import subprocess
from datetime import datetime, timedelta
import pandas as pd

#Load files
def load_files(folder_path):
    # Define the folder path to search for files
    # folder_path = Path('/home/malan/Projects/cto-tool')
    all_files = []
    extensions=('.py', '.js', '.cpp', '.c', '.h', '.css', '.html', '.java', '.rb',
                          '.php', '.ts', '.cjs', '.pyx', '.ini', '.json', '.yaml', '.yml',
                          '.toml', '.cfg', '.md', '.rst', '.txt', '.sh', '.ps1', '.vue')
    # Iterate through the directory structure
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for file in filenames:
            if file.endswith(extensions):
                # Print the full path to each .py file
                # print(os.path.join(dirpath, file))
                all_files.append(os.path.join(dirpath, file))
    return all_files


# Function to remove the files from .venv
def filter_files(file_list):
    filtered_files = [file for file in file_list if '.venv' not in os.path.dirname(file)]
    return filtered_files


#Filter files based on the number of weeks
def filter_data(num_weeks,filtered_files):
    # Calculate the date `num_weeks` ago
    clone_date = pd.to_datetime("2024-09-05")
    date_n_weeks_ago = clone_date - timedelta(weeks=num_weeks)

    branch_name = 'main'
    final_list = []
    
    for relative_path in filtered_files[:]:
        # Construct the save path
        save_path = f'git_log_history_by_files_{branch_name}/' + "_".join(relative_path.split("/")[-2:]) + ".txt"
        
        # Construct the Git command
        command = [
            'git', 'log', '--all', branch_name,
            '--pretty=format:%h ##%an ##%s ##(%ad)', '--date=iso',
            f'--since={date_n_weeks_ago}', relative_path
        ]
        
        # Execute the Git command using subprocess
        result = subprocess.run(command, stdout=subprocess.PIPE, text=True)
        
        # Process the result
        res = result.stdout.strip()
        res += "##" + "*".join(relative_path.split("/")[-2:])
        final_list.append(res)
        res = ""
        
        # Optionally, save to a file
        # with open(save_path, 'w') as f:
        #     f.write(result.stdout)

    return final_list

#Processing the git log data
def process_git_log_data(final_list):
    # Step 1: Filter out entries that have more than 2 '##' separators
    data = [content for content in final_list if len(content.split('##')) > 2]
    
    # Step 2: Process the filtered entries to separate and structure the data
    final_result_list = []
    for entry in data:
        res = entry.split("##")
        if len(res) > 5:
            internal_parts = entry.split(")\n")
            temp_list = [part.split("##") for part in internal_parts]
            final_result_list.extend(temp_list)
        else:
            final_result_list.append(res)

    # Step 3: Create a list of tuples for DataFrame creation
    df_result = []
    for commit in final_result_list:
        if len(commit) == 4:
            id_, name, msg, date = commit
            df_result.append((id_.strip(), name.strip(), msg.strip(), date.strip(), None))
        elif len(commit) == 5:
            id_, name, msg, date, file_name = commit
            df_result.append((id_.strip(), name.strip(), msg.strip(), date.strip(), file_name))
        else:
            # Handle any unexpected formats or log the issue if needed
            pass

    # Step 4: Create and return the DataFrame
    df = pd.DataFrame(df_result, columns=['ID', 'Name', 'Message', 'Date', 'File_Name'])
    return df

#Cleaning the DataFrame
def clean_dataframe(df):
    # Define the function to replace parentheses
    def replace_string(x):
        return x.replace('(', "").replace(')', "")
    
    # Clean the 'Date' column
    df['date_cleaned'] = df['Date'].apply(lambda x: replace_string(x))
    
    # Convert 'date_cleaned' to datetime
    df['date_cleaned'] = pd.to_datetime(df['date_cleaned'], format='ISO8601', errors='coerce')
    
    # Backfill missing 'File_Name' values
    df["file_name_filled"] = df['File_Name'].bfill()
    
    # Drop unnecessary columns
    df = df.drop(columns=['Date', 'File_Name'])
    
    # Rename columns
    df = df.rename(columns={'file_name_filled': 'file_name', 'date_cleaned': 'Date'})
    
    # Drop duplicates based on 'ID' and 'Message'
    df.drop_duplicates(subset=['ID', 'Message'], inplace=True)
    
    return df

#Save the data into csv file
def save_dataframe(df, num_weeks):
    filename=f"git_log_{num_weeks}_weeks.csv"
    df.to_csv(filename, index=False)

folder_path = os.path.dirname(os.getcwd())

all_files= load_files(folder_path)
filtered_files = filter_files(all_files)
#Change the number of weeks
num_weeks=2
final_list= filter_data(num_weeks, filtered_files)
df= process_git_log_data(final_list)
df= clean_dataframe(df)
save_dataframe(df,num_weeks)
