
current_season_sample_results.csv: data compare_drivers.py sort_drivers.py
	source venv/bin/activate; python3 sort_drivers.py > current_season_sample_results.csv; deactivate

data: venv csv_data convert_csv_to_parquet.py calculate_teammate_deltas.py
	test -d data || mkdir data
	source venv/bin/activate; python3 convert_csv_to_parquet.py csv_data; python3 calculate_teammate_deltas.py; deactivate

venv: requirements.txt create_venv.py
	./create_venv.py venv requirements.txt

clean:
	-rm current_season_sample_results.csv

clean_data:
	-rm -rf data

clean_venv:
	-rm -rf venv

clean_all: clean clean_venv clean_data
