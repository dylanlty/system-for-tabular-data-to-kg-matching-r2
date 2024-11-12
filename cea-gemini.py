'''
Round 2 of STI vs LLMs Track - SemTab Challenge 2024
Created in 2024
@author: dylanlty
'''

from pathlib import Path
import time
import csv 
import re
import threading
import google.generativeai as genai
import csv
import pandas as pd 
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from config import Config

# MODEL CONFIGURATION 
GEMINI_API_KEY = "xxxxx"

MODEL_NAME = "gemini-1.5-flash"

genai.configure(api_key=GEMINI_API_KEY)

generation_config = {
  "temperature": 0.25,
  "top_p": 0.95,
  "top_k": 64,
  "max_output_tokens": 8192,
  "response_mime_type": "text/plain",
}

model = genai.GenerativeModel(
  model_name=MODEL_NAME,
  generation_config=generation_config,
)

# INITIALIZATION
entity_dict = {}
event = threading.Event()
 
config = Config["wikidata"]     # or config = Config["dbpedia"] 

# IMPLEMENTATION
def write_csv(wfilename, wrow, wcol, wurl):

    output_filename = "output/cea-round2.csv"

    with open(output_filename, mode='a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([wfilename, wrow, wcol, wurl])
        
def write_temp(data):

    temp_path = "output/csv.temp"

    with open(temp_path, mode='w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Split data into lines
        lines = data.strip().split("\n")
        
        for line in lines:
            # Use csv.reader to handle quoted fields properly
            row = next(csv.reader([line]))
            csv_writer.writerow(row)
    
    return temp_path

def extract_csv(filename: str) -> list:     

    with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.reader(csvfile) 
        return list(csv_reader)

url_pattern = re.compile(r'http[s]?://[^\s",]+')            
def extract_url(text):        

    matches = url_pattern.findall(text)
    
    if matches:
        KG_url = " ".join(matches)
        return KG_url
    else:
        KG_url = " "
        return KG_url

'''
Clean the data using a prompt by the LLM.
It corrects words that are not written properly and output it in the same format it has been input.
'''
def clean_data(raw_csv_data):

    clean_data_msg = f"""
        CSV file: {raw_csv_data}
        I am cleaning the data to perform Knowledge Graph Matching of data cells to WikiData. 
        Perform data cleaning to remove noise, tags and correct any misspelled words.
        Then, if possible, give some context to the cells to enhance the annotation process.
        Provide the response as the same format with no other text. 
        Do not add additional rows. 
        """   
    
    csv_data_response = model.generate_content(clean_data_msg, safety_settings={
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE
            }).text.strip()
    
    return csv_data_response

'''
Up to 5 entities are retrieved from the Lookup API
'''
def get_entities(cell_data):      

    limit = 5
    KG_api = config["KG_api"]

    KG_results = []
    entities = KG_api.getKGEntities(cell_data, limit)

    for ent in entities:
        KG_results.append(ent)

    return KG_results

'''
Annotation process of each cell data with an entity
'''
def process_cell(filename, row_index, col_index, cell_data, csv_data, entity_dict):

    KG_source = config["KG_source"]
    KG_link_template = config["KG_link_template"]

    # Entities are retrieved or added into the dictionary cache
    if cell_data in entity_dict:
        KG_results = entity_dict[cell_data]
    else:
        KG_results = get_entities(cell_data)
        entity_dict[cell_data] = KG_results

    # Automatically annotate when there is only one candiate 
    if len(KG_results) == 1:
        KG_url = extract_url(str(KG_results))     
    
    # LLM performs matching
    else:        
        matching_msg = f"""                         
            CSV file: {str(csv_data)}
            Entities: {str(KG_results)}
            Analyze the entier CSV file and the specific row of data to determine its context.
            Match {cell_data} at row {row_index} with the most suitable {KG_source} link from the options above.
            Provide your final response as the selected {KG_source} link only with no additional text. Do not include any other details in your response.
            Example Format: {KG_link_template}
            If no entities are provided above, use your own knowledge to find the best {KG_source} match.  
            If there is no suitable match, leave your response empty. 
            """  
        
        response = model.generate_content(matching_msg, safety_settings={
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE
            }).text.strip()
        KG_url = extract_url(response).strip()

    print(f"Response for {filename}, {cell_data}, row {row_index}, column {col_index}: {KG_url} \n") 
    
    if KG_url:
            write_csv(filename, row_index, col_index, KG_url)
        
'''
Clear the entity cache dictionary after 15 seconds
'''
def clear_entity_cache(entity_dict):
    
    if event.is_set():
        return 
    entity_dict.clear()
    threading.Timer(15, clear_entity_cache, args=[entity_dict]).start()       

'''
Annotate files in batches 
'''
def annotate_files(target_file_path, input_folderpath):
    
    target_df = pd.read_csv(target_file_path, header=None, names=["filename", "row_index", "col_index"])
    clear_entity_cache(entity_dict)

    # Iterate through each file a the DataFrame
    for filename in target_df["filename"].unique():
        filepath = f"{input_folderpath}/{filename}.csv" 

        # Load the CSV file 
        raw_csv_df = pd.read_csv(filepath, header=None, skiprows=1)
        cleaned_batches = ""

        # Process data in batches of 15 rows
        for start_row in range(0, len(raw_csv_df), 15):

            batch = raw_csv_df.iloc[start_row:start_row + 15]       
            csv_batch = batch.to_csv(header=None, index=False)
            
            # Apply the cleaning function
            cleaned_batch = clean_data(csv_batch) + "\n"            
            cleaned_batches += cleaned_batch

        temp_path = write_temp(cleaned_batches)

        try:
            cleaned_df = pd.read_csv(temp_path, header=None)

        except Exception as e:
            cleaned_df = raw_csv_df
                
        # Process cell data
        for _, row in target_df[target_df["filename"] == filename].iterrows():
            row_id = int(row["row_index"] - 1)
            col_id = int(row["col_index"])

            row_id_target = int(row["row_index"])

            try:
                cell_data = cleaned_df.iloc[row_id, col_id]

            except IndexError as e:
                continue 

            # Data in first row and second row: 
            # Context data will start from the first row of data and ends after adding 4 more rows. 
            try:
                if row_id == 0 or row_id == 1:                      
                    context_start_row = max(0, row_id - 2)
                    context_end_row = min(cleaned_df.shape[0] - 1, row_id + 5)  

                    context_df = cleaned_df.iloc[context_start_row:context_end_row]

                    csv_data = context_df.to_csv(index=False, header=None)
                    process_cell(filename, row_id_target, col_id, cell_data, csv_data, entity_dict)

                # Data in last row and second last row: 
                # Context data will start from the last row of data and ends after adding 4 more rows. 
                elif row_id >= cleaned_df.shape[0] - 2:
                    context_start_row = max(0, row_id - 4)
                    context_end_row = min(cleaned_df.shape[0], row_id + 5)  
                    
                    context_df = cleaned_df.iloc[context_start_row:context_end_row]

                    csv_data = context_df.to_csv(index=False, header=None)
                    process_cell(filename, row_id_target, col_id, cell_data, csv_data, entity_dict)

                else:
                    # Extract 2 rows above and below
                    context_start_row = max(0, row_id - 2)
                    context_end_row = min(cleaned_df.shape[0], row_id + 3) 
                    
                    context_df = cleaned_df.iloc[context_start_row:context_end_row]

                    csv_data = context_df.to_csv(index=False, header=None)
                    process_cell(filename, row_id_target, col_id, cell_data, csv_data, entity_dict)
            
            except Exception as e:
                continue
                

if __name__ == '__main__':
    
    start_time = time.time()
    event.clear()
    input_folderpath = "data/tables"
    target_file_path = "data/gt/cea_target.csv"  

    
    annotate_files(target_file_path, input_folderpath)

    event.set()     # stop threading timer

    print('\n\nTime taken:', time.time() - start_time, '\n\n')