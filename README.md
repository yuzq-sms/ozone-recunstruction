
Overview
This project contains tools for meteorological data processing, analysis, and modeling. The workflow includes data preprocessing, model training, data reconstruction, and feature importance analysis.

File Structure

Notebooks:
• `main.ipynb`: Main entry point for experimental analysis and model training

• `reconstruct.ipynb`: Data reconstruction module

• `factor_weight.ipynb`: Calculates meteorological variable weights using Random Forest method


Data Preparation
• `data_prepare/`: Folder containing data preprocessing scripts

  • Includes data cleaning, normalization, and feature engineering utilities


Usage Guide

1. Environment Preparation
   • Create and activate conda environment using above commands


2. Data Preparation
   • Run scripts in `data_prepare/` to preprocess raw data


3. Main Analysis
   • Open `main.ipynb` for:

   • Exploratory data analysis

   • Model training (includes LightGBM and scikit-learn estimators)

   • Performance evaluation


4. Additional Tools
   • Use `reconstruct.ipynb` for data reconstruction

   • Run `factor_weight.ipynb` for variable importance analysis


Core Dependencies
• Python 3.9

• Machine Learning: LightGBM, scikit-learn

• Data Processing: pandas, numpy

• Visualization: matplotlib, seaborn

• Notebook: Jupyter
