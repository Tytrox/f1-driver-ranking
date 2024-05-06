
data: venv csv_data convert_csv_to_parquet.py
	test -d data || mkdir data
	source venv/bin/activate; python3 convert_csv_to_parquet.py csv_data; python3 calculate_teammate_deltas.py; deactivate

venv: requirements.txt create_venv.py
	./create_venv.py venv requirements.txt

clean:

clean_data:
	rm -rf data

clean_venv:
	rm -rf venv

clean_all: clean_venv clean_data
