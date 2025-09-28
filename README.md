# BASH


python -m venv .venv
# Macbook
source .venv/bin/activate  
# Windows: .venv\Scripts\activate
pip install -r requirements.txt
mkdir data
mkdir results

cp {path to sample_data.csv} ./data/


# 10M file
python generate_data.py --rows 10000000 --out data/transaction_10M.csv --start 2024-01-01 --months 12 --gzip

# top spender
python top_spenders.py --input data/transaction_10M.csv --output results/top_spenders.csv --chunksize 100000
