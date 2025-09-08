
.PHONY: wb-metadata wb-metadata-csv wb-metadata-keyed wb-config all
all: wb-metadata wb-metadata-csv wb-metadata-keyed

wb-metadata:
	python _programs/make_wb_metadata_yaml.py

wb-metadata-csv:
	python _programs/make_wb_metadata_csv.py

wb-metadata-keyed:
	python _programs/make_wb_metadata_yaml_keyed.py

wb-config:
	python _programs/run_from_config.py
