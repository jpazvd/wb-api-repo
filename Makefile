
.PHONY: wb-metadata wb-metadata-csv all
all: wb-metadata wb-metadata-csv

wb-metadata:
	python _programs/make_wb_metadata_yaml.py

wb-metadata-csv:
	python _programs/make_wb_metadata_csv.py


.PHONY: wb-config
wb-config:
	python _programs/run_from_config.py
