*******************************************************************************
* wbopendata
*! v 17.4.0  	 23May2026               by Joao Pedro Azevedo
* 	v17.4.0: Phase 6 — describe (metadata-only) + linewrap features:
* 	         linewrap() / maxlength() / linewrapformat() — wrap long
* 	         metadata strings for publication graphs.
* 	         describe — fetch metadata only (no data download); routes
* 	         to new __wbod_query_metadata (v18) with linewrap opts.
* 	         Calls new __wbod_linewrap / __wbod_metadata_linewrap /
* 	         __wbod_query_metadata helpers.
* 	         Deferred to Phase 6.1: noCHAR enforcement (the actual
* 	         `char define wbopendata_*' writes inside the data-fetch
* 	         path); language() wiring verification.
* 	v17.3.0: Phase 5 — basic country context on-by-default:
* 	         noBASIC opts out of the 8-field basic context auto-merge.
* 	         noCHAR opts out of dataset characteristic embedding
* 	         (char-write block itself lands with Phase 5.1 / Phase 6).
* 	         Both call sites (match() flow + default flow) retargeted
* 	         to new __wbod_countrymetadata (v18 — adds basic + geo
* 	         flags); legacy _countrymetadata kept for update paths.
* 	v17.2.0: Phase 4 — cache management + sync replace (apply) path:
* 	         clearcache / cacheinfo / checkupdate / cleardatacache /
* 	         resetdatacache + nocache + cachedays() + forcestata /
* 	         forcepython + sync replace flow (preview → apply → diff).
* 	         Calls new __wbod_cache / __wbod_sync / __wbod_sync_diff /
* 	         __wbod_refresh_yaml / __wbod_write_stats_history helpers.
* 	         Backward-compat aliases: syncforce / syncpreview / syncdryrun.
* 	v17.1.0: Phase 3 — discovery commands wired in dispatcher:
* 	         sources / allsources / alltopics / search() / info() / sync (dryrun)
* 	         Calls new __wbod_* helpers (Tiers 1-3 + transitive deps).
* 	v17.0:  Create region metadata (24Jan2023)
*******************************************************************************

program def wbopendata, rclass

version 9.0

    syntax                                          ///
                 [,                                 ///
                         LANGUAGE(string)           ///
                         COUNTRY(string)            ///
                         TOPICS(string)             ///
                         INDICATORs(string)         ///
                         YEAR(string)               ///
						 DATE(string)				///
						 SOURCE(string)				///
 						 PROJECTION					///						 
                         LONG                       ///
                         CLEAR                      ///
                         LATEST                     ///
                         NOMETADATA                 ///
						 UPDATE						///
						 QUERY						///
						 CHECK						///
						 NOPRESERVE					///
						 PRESERVEOUT				///
						 COUNTRYMETADATA			///
						 ALL						///
						 BREAKNOMETADATA			///
						 METADATAOFFLINE			///
						 FORCE						///
						 SHORT						///
						 DETAIL						///
						 CTRYLIST					///
						 MATCH(string)				///
							ISO					///
							REGIONS				///
							ADMINR				///
							INCOME				///
							LENDING				///
							CAPITALS			///
							BASIC				///
							FULL				///
							countrycode_iso2 	///
							region 				///
							region_iso2 		///
							regionname 			///
							adminregion 		///
							adminregion_iso2 	///
							adminregionname 	///
							incomelevel 		///
							incomelevel_iso2 	///
							incomelevelname 	///
							lendingtype 		///
							lendingtype_iso2 	///
							lendingtypename 	///
							capital 			///
							latitude 			///
							longitude 			///
							countryname			///
							SOURCES                    ///
							ALLSOURCES                 ///
							ALLTOPICS                  ///
							SEARCH(string)             ///
							LIMIT(string)              ///
							PAGE(string)               ///
							SEARCHSOURCE(string)       ///
							SEARCHTOPIC(string)        ///
							SEARCHFIELD(string)        ///
							EXACT                      ///
							INFO(string)               ///
							SYNC                       ///
							REPLACE                    ///
							SYNCFORCE                  ///
							SYNCPREVIEW                ///
							SYNCDRYRUN                 ///
							CHECKUPDATE                ///
							CLEARCACHE                 ///
							CACHEINFO                  ///
							CLEARDATACACHE             ///
							RESETDATACACHE             ///
							NOCACHE                    ///
							CACHEDAYS(integer 7)       ///
							FORCESTATA                 ///
							FORCEPYTHON                ///
							noBASIC                    ///
							noCHAR                     ///
							LINEWRAP(string)           ///
							MAXLENGTH(string)          ///
							LINEWRAPFORMAT(string)     ///
							DESCRIBE                   ///
                 ]


	quietly {
	
	
**********************************************************************************


local indicator `indicators'

	* ------------------------------------------------------------------
	* Phase 5 (v17.3.0): basic country context on-by-default
	* ------------------------------------------------------------------
	* `basic' is set unless user explicitly opted out via noBASIC. The
	* downstream call to __wbod_countrymetadata passes it through and
	* will merge the 8-field basic context (region / regionname /
	* adminregion / adminregionname / incomelevel / incomelevelname /
	* lendingtype / lendingtypename) into the working dataset.
	if ("`basic'" == "") local basic "basic"
	* `char' default ON; noCHAR suppresses dataset characteristic embed
	* (the actual char-write block lands with Phase 5.1 / Phase 6).
	if ("`char'" == "") local char "char"

	* ------------------------------------------------------------------
	* Phase 3 (v17.1.0): discovery commands + dryrun sync
	* ------------------------------------------------------------------
	local limit_specified = ("`limit'" != "")
	local limit_val = 20
	if (`limit_specified') {
		local limit_val = real("`limit'")
		if (missing(`limit_val') | `limit_val' <= 0) local limit_val = 20
	}
	local page_val = 1
	if ("`page'" != "") {
		local page_val = real("`page'")
		if (missing(`page_val') | `page_val' < 1 | `page_val' != int(`page_val')) {
			di as err "option page() incorrectly specified -- must be a positive integer"
			exit 198
		}
	}

	local has_search_filter = ("`searchsource'" != "" | "`searchtopic'" != "")
	if ("`sources'" != "" | "`allsources'" != "" | "`alltopics'" != "" | "`search'" != "" | `has_search_filter' | "`info'" != "") {
		if ("`sources'" != "") {
			noisily __wbod_sources, limit(`limit_val')
			return add
			exit _rc
		}
		if ("`allsources'" != "") {
			if (`limit_specified') noisily __wbod_sources, limit(`limit_val')
			else                   noisily __wbod_sources
			return add
			exit _rc
		}
		if ("`alltopics'" != "") {
			if (`limit_specified') noisily __wbod_topics, limit(`limit_val')
			else                   noisily __wbod_topics
			return add
			exit _rc
		}
		if ("`search'" != "" | `has_search_filter') {
			noisily __wbod_search "`search'", limit(`limit_val') page(`page_val') ///
				source("`searchsource'") topic("`searchtopic'") ///
				field("`searchfield'") `exact' `detail'
			return add
			exit _rc
		}
		if ("`info'" != "") {
			capture noisily __wbod_info, indicator("`info'")
			if (_rc == 0) return add
			exit _rc
		}
	}

	* ------------------------------------------------------------------
	* Phase 6 (v17.4.0): describe (metadata-only) + linewrap features
	* ------------------------------------------------------------------
	* Assemble linewrap option pass-through once; consumed by describe
	* below and (eventually) by other metadata callers.
	local _lw_opts ""
	if ("`linewrap'" != "")        local _lw_opts `_lw_opts' linewrap("`linewrap'")
	if ("`maxlength'" != "")       local _lw_opts `_lw_opts' maxlength("`maxlength'")
	if ("`linewrapformat'" != "")  local _lw_opts `_lw_opts' linewrapformat("`linewrapformat'")

	if ("`describe'" != "") {
		if ("`indicators'" == "" & "`indicator'" == "") {
			di as err "describe requires indicator() or indicators()"
			exit 198
		}
		local _ind_arg = cond("`indicator'" != "", "`indicator'", "`indicators'")
		noisily __wbod_query_metadata, ind("`_ind_arg'") language("`language'") `_lw_opts'
		return add
		exit _rc
	}
	* ------------------------------------------------------------------

	* ------------------------------------------------------------------
	* Phase 4 (v17.2.0): cache management + sync replace (apply) path
	* ------------------------------------------------------------------

	* Resolve backward-compatible aliases into canonical modifiers (deprecated v18.0):
	*   syncforce   → sync + replace + force
	*   syncpreview → sync + replace
	*   syncdryrun  → sync (dryrun is the default)
	if ("`syncforce'" != "") {
		noi di as txt "{bf:Note:} {cmd:syncforce} is deprecated; use {cmd:sync replace force} instead."
		local sync "sync"
		local replace "replace"
	}
	if ("`syncpreview'" != "") {
		noi di as txt "{bf:Note:} {cmd:syncpreview} is deprecated; use {cmd:sync replace} instead."
		local sync "sync"
		local replace "replace"
	}
	if ("`syncdryrun'" != "") {
		noi di as txt "{bf:Note:} {cmd:syncdryrun} is deprecated; use {cmd:sync} instead."
		local sync "sync"
	}

	* Data-cache maintenance commands (standalone exits)
	if ("`cleardatacache'" != "") {
		__wbod_cache, cleardatacache
		exit 0
	}
	if ("`resetdatacache'" != "") {
		__wbod_cache, resetdatacache
		exit 0
	}

	* Metadata-cache + sync routing
	if ("`sync'" != "" | "`checkupdate'" != "" | "`clearcache'" != "" | "`cacheinfo'" != "") {
		if ("`clearcache'" != "") {
			__wbod_cache, clear
			exit _rc
		}
		if ("`cacheinfo'" != "") {
			__wbod_cache, info
			exit _rc
		}
		if ("`checkupdate'" != "") {
			__wbod_cache, checkversion
			if (r(needs_update)) {
				di as result "Update available!"
				di as text "  Local version:  v" r(local_version)
				di as text "  Remote version: v" r(remote_version)
				di as text ""
				di as text `"Run {stata wbopendata, sync replace:wbopendata, sync replace} to update"'
			}
			else di as text "Metadata is up-to-date (v" r(local_version) ")"
			exit _rc
		}

		* sync: always show preview first
		noi __wbod_sync_preview, `detail'
		return add

		* sync without replace: dryrun (safe default) — stop after preview
		if ("`replace'" == "") {
			di as text ""
			if ("`force'" != "") {
				di as text `"To apply changes, run: {stata wbopendata, sync replace force:wbopendata, sync replace force}"'
			}
			else {
				di as text `"To apply changes, run: {stata wbopendata, sync replace:wbopendata, sync replace}"'
			}
			exit 0
		}

		* sync replace: actually apply the sync
		di as text ""
		di as text "Proceeding with sync..."
		di as text ""
		* Snapshot current indicator list for post-sync diff
		tempfile _sync_snap
		capture quietly __wbod_sync_diff, before("`_sync_snap'")
		if ("`forcestata'" != "") __wbod_sync, forcestata `force'
		else if ("`forcepython'" != "") __wbod_sync, forcepython `force'
		else if ("`force'" != "") __wbod_sync, force
		else __wbod_sync
		local sync_rc = _rc
		if (`sync_rc' == 0) {
			* Get counts after sync for history
			quietly __wbod_sync_preview
			local ind_count = r(ind_count)
			local src_count = r(src_count)
			local top_count = r(top_count)
			local ctry_count = r(ctry_count)
			local method = r(cache_method)
			local by_source = r(by_source)
			local by_topic = r(by_topic)
			if ("`method'" == "") local method = "unknown"
			capture quietly __wbod_write_stats_history, ///
				method("`method'") ///
				indicators(`ind_count') ///
				sources(`src_count') ///
				topics(`top_count') ///
				countries(`ctry_count') ///
				bysource("`by_source'") ///
				bytopic("`by_topic'")
			* Show indicator diff vs pre-sync snapshot
			capture noisily __wbod_sync_diff, after("`_sync_snap'")
		}
		exit `sync_rc'
	}
	* ------------------------------------------------------------------

	* query and check can not be selected at the same time
		if ("`query'" == "query") & ("`check'" == "check") {
			noi di  as err "update query and update check options cannot be selected at the same time."
			exit 198
		}
	
		set checksum off
	
	* update : update query / does not triger the download of any data
		if ("`update'" == "update") & wordcount("`query' `check' `countrymetadata' `all'")==0 {
		
			noi wbopendata, update query
			break
		}
		
	* update : update query / triger the download of selected data
	* update : force  - creates new help files and metadata documentation by source and topics
	* trigger: _parameters
	* triggers _update indicators.ado
	*		refresh Source
	*		refresh Indicators
	
		if ("`update'" == "update") & wordcount("`query' `check' `countrymetadata' `all'")== 1 {

			noi _update_wbopendata, update `query' `check'	`countrymetadata' `all' `force' `short' `detail' `ctrylist'
			break
					
		}

	* metadataoffline options
	* this option will refress all meatadata and generate 71 files with all metadata indicators by source id and topic id.
		if ("`metadataoffline'" == "metadataoffline") {

			noi _update_wbopendata, update force all
			local update "update"
			local force  "force"
			local all    "all"
			break
					
		}
		
**********************************************************************************
* option to match	
	
	
	qui if ("`match'" != "") {

		__wbod_countrymetadata, match(`match') `full' `iso' `basic' `geo' `isolist' `regionlist' `adminlist' `incomelist' `lendinglist' `capitalist' `isolist' `countryname' `region'  `region_iso2' `regionname' `adminregion' `adminregion_iso2' `adminregionname' `incomelevel' `incomelevel_iso2' `incomelevelname'  `lendingtype' `lendingtype_iso2' `lendingtypename' `capital' `longitude' `latitude'

	}

**********************************************************************************
	
	
		local f = 1

		if ("`indicator'" != "") & ("`update'" == "") & ("`match'" == "") {

			_tknz "`indicator'" , parse(;)

			forvalues i = 1(1)`s(items)'  {

			   if ("``i''" != ";") &  ("``i''" != "") {

				   tempfile file`f'

				   noi _query ,       language("`language'")      		///
										 country("`country'")         	///
										 topics("`topics'")           	///
										 indicator("``i''")             ///
										 year("`year'")               	///
										 date("`date'")					///
										 source("`source'")				///
										`projection'					///
										 `long'                       	///
										 `clear'                      	///
										 `nometadata'
					local time  "`r(time)'"
					local namek "`r(name)'"


					if ("`nometadata'" == "") & ("`indicator'" != "") {
						cap: noi _query_metadata  , indicator("``i''")                  /*  Metadata   */
						local qm1rc = _rc
						if (`qm1rc' != 0) {
							noi di ""
							noi di as err "{p 4 4 2} Sorry... No metadata available for " as result "`indicator'. {p_end}"
							noi di ""
							if ("`breaknometadata'" != "") {
								break
								exit 21
							}
						}
					}

					local w1 = word("``i''",1)
					return local varname`f'     = trim(lower(subinstr(word("`w1'",1),".","_",.)))
					return local indicator`f'  "`w1'"
					return local topics`f'     "`topics'"
					return local year`f'       "`year'"
					return local source`f'     "`r(source)'"
					return local varlabel`f'   "`r(varlabel)'"
					return local time`f'       "`time'"

					local namek = trim(lower(subinstr(word("`w1'",1),".","_",.)))

					if ("`long'" != "") {
						sort countrycode `time'
					}

					save `file`f''

					local f = `f'+1

				}
				
				local name "`name' `namek'"

			}

		}

		 else {

			if ("`update'" == "") & ("`match'" == "") {
			 
				noi _query , language("`language'")       	///
									country("`country'")    ///
									topics("`topics'")      ///
									indicator("``i''")      ///
									year("`year'")          ///
									date("`date'")			///
									source("`source'")		///
									`projection'			///
									`long'                  ///
									`clear'                 ///
									`latest'                ///
									`nometadata'
				local time  "`r(time)'"
				local name "`r(name)'"


				if ("`nometadata'" == "") & ("`indicator'" != "") {
					cap: noi _query_metadata  , indicator("``i''")                  /*  Metadata   */
					local qm2rc = _rc
					if ("`qm2rc'" == "") {
						noi di ""
						noi di as err "{p 4 4 2} Sorry... No metadata available for " as result "`indicator'. {p_end}"
						noi di ""
						if ("`breaknometadata'" != "") {
							break
							exit 22
						}
					}
				}
				
			}

			local w1 = word("`indicator'",1)
			return local varname1     = trim(lower(subinstr(word("`w1'",1),".","_",.)))
			return local indicator1  "`w1'"
			return local country1    "`country'"
			return local topics1     "`topics'"
			return local year1       "`year'"
			return local source1     "`r(source)'"
			return local varlabel1   "`r(varlabel)'"
			return local time1       "`time'"

			local name = trim(lower(subinstr(word("`w1'",1),".","_",.)))
			
		}

		return local indicator  "`indicator'"
		local f = `f'-1

		if (`f' != 0) {

			if ("`long'" != "") {
				use `file1'
				forvalues i = 2(1)`f'  {
					merge countrycode year using `file`i''
					drop _merge
					sort countrycode `time'
				}
			}

			if ("`long'" == "") {
				use `file1'
				forvalues i = 2(1)`f'  {
					append using `file`i''
				}
			}
		}

		if ("`latest'" != "") &  ("`long'" != "") {
		    
			* check if name is to long. 
		    local length_name = length("`name'")
			* shorten name if too long
			if (`length_name' > 20) {
				local name = substr("`name'",1,20)
				return local name "`name'"
			}
			
			tempvar tmp
			egen `tmp' = rowmiss(`name'_)
			keep if `tmp' == 0
			sort countryname countrycode `time'
			bysort countryname countrycode : keep if _n==_N
		}

	}
	
	local nametmp  = "`indicator'"
	local nametmp = lower("`nametmp'")
	local nametmp = subinstr("`nametmp'",";"," ",.)	
	local nametmp = subinstr("`nametmp'",".","_",.) 
	return local name "`nametmp'"
	
**********************************************************************************
	

	qui if ("`update'" == "") {

		tostring  countryname countrycode, replace

		__wbod_countrymetadata, match(countrycode) `full' `iso' `basic' `geo' `countrycode_iso2' `region' `region_iso2' `regionname' `adminregion' `adminregion_iso2' `adminregionname' `incomelevel' `incomelevel_iso2' `incomelevelname' `lendingtype' `lendingtype_iso2' `lendingtypename' `capital' `longitude' `latitude' `countryname'

	}
	
**********************************************************************************
	
	
	if ("`nopreserve'" == "") {
		return add
	}
	
end


*******************************************************************************
*  v 16.3  	8Jul2020               by Joao Pedro Azevedo
* 	change API end point to HTTPS
*******************************************************************************
**********************************************************************************
*  v 16.2.3    29Jun2020 				by Joao Pedro Azevedo
*	 rewrote query metadata. It now uses _api_read.ado
**********************************************************************************
*  v 16.2.2    28Jun2020 				by Joao Pedro Azevedo
*	 changed server used to query metadata
***********************************************************************************
*  v 16.2.1    14Apr2020 				by Joao Pedro Azevedo
*    add flow check before runing _query.ado / _query.ado should not run if 
*    metadataoffline option is selected.
**********************************************************************************
*  v 16.2      13Apr2020 				by Joao Pedro Azevedo
*    create option metadataoffline 
*       generates SORUCEID and TOPICID metadata in local installation
*       71 sthlp files are generated and 15mb of documentation is created
**********************************************************************************
*  v 16.1      12Apr2020 				by Joao Pedro Azevedo
*	remove metadata of SOURCID and TOPICSID from the main dissemination package                                                     
**********************************************************************************
*  v 16.0.1    31Oct2019               by Joao Pedro Azevedo 
 * improve a few small functionalities
**********************************************************************************
*  v 16.0	    27Oct2019               by Joao Pedro Azevedo 
* created and tested new functions, namely:
*  _api_read_indicators.ado : download indicator list from API, for formats 
*    output in a Stata readable form
*  _update_indicators.ado: calls _api_read_indicators.ado, and uses its output to  
*  generate additioanl documentation 
*  outputs for wbopendata:
*     dialogue indicator list
*     sthlp indicator list by Source and Topic
*     sthlp indicator metadata by Source and Topic
*  match option supported in wbopendata (add countrymetadata matching on MATCH var) 
* _website.ado : screens a text file and converts and http or www "word" to a SMCL 
*    web compatible code.
* _parameters.ado: now include detailed count of indicators by SOURCE and TOPIC
* _wbopendata.ado: renamned _update_wbopendata
* _indicator: renamed _update_indicators
* _update_wbopendata.ado: now checks for changes at the SOURCE or TOPIC level
* fixed return list when multiple indicators are selected
* updated help file to allow for the search of indicators by Source and Topics
**********************************************************************************
*  v 15.1	    04Mar2019               by Joao Pedro Azevedo 
*	New Features
*		new error categories to faciliate debuging
*		error 23: series no longer supported moved to archive
*		country attribute table fully revised and linked to api
*		update check, update query, and update
*		auto refresh indicators
*		revised _wbopendata.ado 		
*		update query; update check; and update options are included
* 		country attributes revised
*		update countrymetadata option created
*		country metadata documentation in help file revised
*		break code when no metadata is available is now an option
*   Revisions
*       over 16,000 indicators
**********************************************************************************
*  v 15.0.1		8Fev2019				by Joao Pedro Azevedo
**********************************************************************************
*  v 15.0	    2Fev2019               	by Joao Pedro Azevedo 
**********************************************************************************
*  v 14.3 	2Feb2019               by Joao Pedro Azevedo 
* 	Bug Fixed
*		_wbopendata_update.ado revised; out.txt file no longer created
**********************************************************************************
*  v 14.2 	31Jan2019               by Joao Pedro Azevedo 
* Bug Fixed
	* update _wbopendata_update.ado
	* set checksum off
**********************************************************************************
*  v 14.1 	19Jan2019               by Joao Pedro Azevedo 
* 	New options: 
     * indicator update function
     * nopreserve option (return list is can be preserved)
* 	Bugs fixed
    * latest option
    * _query_metadata.ado (source id return list) fixed
* 	Revisions
     * examples
     * update help file
     * list of indicators
**********************************************************************************
*  v 14.0  14Jan2019               by Joao Pedro Azevedo 
*		revised indicator list
*		change to new API server 
**********************************************************************************
*  v 13.4  01jul2014               by Joao Pedro Azevedo                        *
*       long reshape
**********************************************************************************
*  v 13.3  30june2014               by Joao Pedro Azevedo                        *
*       new error control (clear option)
**********************************************************************************
*  v 13.2  24june2014               by Joao Pedro Azevedo                        *
*       new error control
**********************************************************************************
*  v 13.1  23june2014               by Joao Pedro Azevedo                        *
*       regional code, name and iso2code
**********************************************************************************
*  v 13  20june2014               by Joao Pedro Azevedo                        *
* 		fix the dups problem                                                    *
*       improve the error messages                                              *
*       update the list of indicators to 9960                                 *
**********************************************************************************
*  v 12  31jan2013               by Joao Pedro Azevedo                        *
*       update to 7349 indicators
*       return list include variable name and label
**********************************************************************************
