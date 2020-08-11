This folder contains all the Code regarding the Data collected/cleaned/processed/analysed for the project.

The main folder consists of all the Jupyter Notebooks as yu can see.
Out of those, given below are the main Jupyter Notebooks that we're using for the Data Collection and Pre-processing tasks.
             -> CLEANED_SummaryStatistics.ipynb (Data from Johns Hopkins API)
             -> IrelandCOVIDAPI.ipynb (Ireland Specific Data from data.gov.ie)
             -> Merged_Data_df.ipynb (Merged Data from Yatko API and Johns Hopkins API)
             -> Pre_processing.ipynb (Pre-processing)
             -> Data_pre_processing_2.ipynb (Pre-processing)
             
BAT files and Python files contain 5 files each. 

These 5 python files are our main scripts that run daily at a fixated time via cron jobs using Windows Task Scheduler.
Python files -> CLEANED_SummaryStatistics.py (Data from Johns Hopkins API)
             -> IrelandCOVIDAPI.py (Ireland Specific Data from data.gov.ie)
             -> Merged_Data_df.py (Merged Data from Yatko API and Johns Hopkins API)
             -> Pre_processing.py (Pre-processing)
             -> Data_pre_processing_2.py (Pre-processing)

