*! debug_yaml_read.do
clear all
set more off

local root "C:/GitHub/myados/wbopendata-dev"
adopath ++ "`root'/src/y"
adopath ++ "`root'/src/_"

di "=== Which yaml ==="
which yaml

di _n "=== Test yaml read with indicators ==="
local yaml_path "`root'/src/_/_wbopendata_indicators.yaml"
di "Path: `yaml_path'"

yaml read using "`yaml_path'", indicators replace
di "Rows: `=_N'"
ds

exit 0
