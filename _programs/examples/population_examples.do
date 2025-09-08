
/* Stata population_examples.do */
capture which wbopendata
if _rc { ssc install wbopendata, replace }

wbopendata, indicator(SP.POP.0004.MA SP.POP.0004.FE SP.POP.0509.MA SP.POP.0509.FE) ///
    clear long date(2000:2050)
export delimited using "_data/wb/pop_counts_demo.csv", replace

wbopendata, indicator(SP.POP.0004.MA.5Y SP.POP.0004.FE.5Y SP.POP.0509.MA.5Y SP.POP.0509.FE.5Y) ///
    clear long date(2000:2050)
export delimited using "_data/wb/pop_shares_demo.csv", replace
