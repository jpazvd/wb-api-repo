
#!/usr/bin/env bash
set -euo pipefail
python "$(dirname "$0")/../wb_api_tools.py" data   --indicators SP.POP.0004.MA,SP.POP.0004.FE,SP.POP.0509.MA,SP.POP.0509.FE   --countries all   --date 2000:2050   --long   --out "$(dirname "$0")/../../_data/wb/pop_age_sex_counts_long.csv"

python "$(dirname "$0")/../wb_api_tools.py" data   --indicators SP.POP.0004.MA.5Y,SP.POP.0004.FE.5Y   --countries BRA,IND,ZAF   --date 2000:2050   --long   --out "$(dirname "$0")/../../_data/wb/pop_age_sex_shares_long.csv"
