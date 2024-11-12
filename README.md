## A System for Tabular data to Knowledge Graph Matching
## STI vs LLMs Track of SemTab 2024 Challenge - Round 2

The code was executed using Python 3.9.13 and on a virtual environment (virtualenv).

Insert the API key before execution. 

- **Execute matching process file**: py cea-gemini.py

Dataset: https://zenodo.org/records/11031987 (MammoTab 24)

# Evaluate CEA task with WikiData Entities
- **CEA_WD_Evaluator**: py CEA_WD_Evaluator.py

# Changes Done 
(from Round 1 - https://github.com/dylanlty/system-for-tabular-data-to-kg-matching/)
- Refined prompts
- Set response safety of prompts to None
- Process the cleaning of data by the LLM in batches of 15 instead all in one 
- Provide few rows for context during the matching process by the LLM rather than the whole table  






