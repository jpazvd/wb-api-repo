*! test_yaml_check.do - Test yaml.ado dependency check

clear all
set more off

local root = c(pwd)
adopath ++ "`root'/src/y"
adopath ++ "`root'/src/_"

di "=== Test 1: Check yaml.ado directly ==="
_wbopendata_check_yaml, minversion(1.9.0)
di "Installed: `r(installed)'"
di "Version: `r(version)'"
di "Source: `r(source)'"
di "Needed install: `r(needed_install)'"

di _n "=== Test 2: Check via v2 parser ==="
* Reset session check
macro drop _wbod_yaml_checked
local yaml_path "`root'/src/_/_wbopendata_indicators.yaml"
timer clear 1
timer on 1
__wbod_parse_yaml_ind_v2 "`yaml_path'"
timer off 1
qui timer list 1
di "Parse time: `r(t1)'s"
di "Rows: `=_N'"

di _n "=== Test 3: Second call (yaml check cached) ==="
timer clear 2
timer on 2
__wbod_parse_yaml_ind_v2 "`yaml_path'"
timer off 2
qui timer list 2
di "Parse time: `r(t2)'s (should skip yaml check)"

di _n "=== All tests passed ==="
exit 0
