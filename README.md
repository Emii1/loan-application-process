# Loan Application Process
This repository contains a process mining analysis of a loan application dataset (BPI Challenge 2017) using PM4Py.

## Overview

The analysis I conducted includes:

- Reading the event log with PM4Py.
- Converting the log to a pandas DataFrame for inspection and basic statistics.
- Calculating basic event log properties:
  - Total cases: 31 509
  - Unique resources: 149
  - Total events recorded: 1 202 267
  - Unique activities: 26
  - Total process variants: 15 930
  - Activity frequency distribution
  - Average cycle times and case lengths
- Process discovery:
  - **Heuristics Miner**
  - **Alpha Miner**
- Model evaluation metrics:
  - Fitness
  - Precision
  - Generalization
  - Simplicity
- Filtering the log by keeping complete events and generating a filtered Heuristics Miner model.
- Process models are saved as images (`.png`) and a BPMN model is also added.


## How to run
* Clone the repository: git clone https://github.com/Emii1/loan-application-process.git
* Open jupyter notebook: jupyter notebook 6_log_interpreter.ipynb

