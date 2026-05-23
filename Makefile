
.PHONY: wb-metadata wb-metadata-csv wb-metadata-keyed wb-config all
all: wb-metadata wb-metadata-csv wb-metadata-keyed

wb-metadata:
	python src/py/make_wb_metadata_yaml.py

wb-metadata-csv:
	python src/py/make_wb_metadata_csv.py

wb-metadata-keyed:
	python src/py/make_wb_metadata_yaml_keyed.py

wb-config:
	python src/py/run_from_config.py
