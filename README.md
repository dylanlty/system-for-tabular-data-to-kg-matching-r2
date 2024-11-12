## A System for Tabular data to Knowledge Graph Matching
## STI vs LLMs Track of SemTab 2024 Challenge - Round 2

The code was executed using Python 3.9.13 and on a virtual environment (virtualenv).

Insert the API key before execution. 

- **Execute matching process file**: py cea-gemini.py

Dataset: https://zenodo.org/records/11031987 (MammoTab 24)

# Evaluate CEA task with WikiData Entities
- **CEA_WD_Evaluator**: py CEA_WD_Evaluator.py

# Changes Made 
(compared to the System for Round 1 - https://github.com/dylanlty/system-for-tabular-data-to-kg-matching/)
- Refined prompts.
- Set response safety of prompts to "None".
- Processed the cleaning of data by the LLM in batches of 15 instead as a whole.
- Provided few rows for context during the matching process by the LLM, rather than using the entire table.
- Used Gemini-1.5-Flash instead of GPT-4o-Mini.






