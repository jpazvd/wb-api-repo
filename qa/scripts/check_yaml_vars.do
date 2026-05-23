*! check_yaml_vars.do - Debug yaml collapse variable names

clear all
set more off

local root = c(pwd)
adopath ++ "`root'/src/y"
adopath ++ "`root'/src/_"

local yaml_path "`root'/src/_/_wbopendata_indicators.yaml"

di "=== Checking yaml bulk collapse variable names ==="
yaml read using "`yaml_path'", indicators replace blockscalars strl

di "Variables produced:"
ds

di _n "Describe:"
describe

exit 0
