
.PHONY: wb-metadata wb-metadata-csv wb-metadata-keyed wb-config wb-update-metadata install test demo all
all: wb-metadata wb-metadata-csv wb-metadata-keyed

install:
	python -m pip install -e ".[test]"

test:
	PYTHONIOENCODING=utf-8 python -m pytest tests/ -v

demo:
	PYTHONIOENCODING=utf-8 python examples/demo_pr_b_c.py

wb-metadata:
	python -m wb_api_tools.make_wb_metadata_yaml

wb-metadata-csv:
	python -m wb_api_tools.make_wb_metadata_csv

wb-metadata-keyed:
	python -m wb_api_tools.make_wb_metadata_yaml_keyed

wb-config:
	python -m wb_api_tools.run_from_config

wb-update-metadata:
	python -m wb_api_tools.update_metadata
