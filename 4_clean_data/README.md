## 4. Clean data
Contains source code for the data cleaning process

### Documentation
[Documentation for entity resolution process](https://docs.google.com/document/d/1L56lpYwiMWjR2xJ-L0s3YYuGczSRAcNlWROz7kVNCzI/edit?usp=sharing)

--- 

### Code Files
- **4_1_clean_names.Rmd** - clean variables for names in the dataset (name, caregiver) 
- **4_2_standardize_vacname.Rmd** - standardize vacname, create pathogen dataset and decouple data
- **4_3_entity_resolution_model.Rmd** - train a Fellegi & Sunter model and inspection process after predicting matched pairs using the trained model
- **4_4_unify_pids.ipynb** - group matched pids into clusters and choose representative pid (pid used for the latest vaccination) for each cluster 
- **4_5_standardize_pid.Rmd** - map pids to the representative pid and re-run deduplication on the available datasets

- **group_pids.py** - helper functions for **4_4_unify_pids.ipynb**

--- 

### Folders
**linked_data** - datasets of linked pids\
 ┣ **deterministic_linking_pairs.parquet** - pid pairs linked using deterministic rule \
 ┣ **full_linked_pairs.parquet** - pid pairs linked using model\
 ┗ **post_inspection_linked_pairs.parquet** - linked pairs after manual inspection
 
 **model_visualization**\
 ┣ **m_u_parameters.html** - graphical representation of m and u values of each agreement level for each variable\
 ┗ **parameter_estimate.html** - m probability as log odds for each variable
 
**manual_inspection** - files recording mis-classified pairs after manual inspection\
 ┣ **false_negative.csv** - pairs mis-classified as not match\
 ┗ **false_positive.csv** - pairs mis-classified as match

 
 

