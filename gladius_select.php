<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# SELECT command interpretation
#

if (!defined('_G__FIELD'))
	define('_G__FIELD', '(\\w+(\\.((\\w+)|\\*))?)|(\\*)');

	$fields = array();	// mapped as (field_name, alias, is_function)
	// (1) get the selected fields
	while (preg_match('/\\s*((\\w+\\s*\\([^\\)]*\\))|'._G__FIELD.')(\\s+(as\\s+)?(\\w+))?\\s*(,)?/iA',
		$this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {

		$last = count($m)-1;
		$this->offset = $m[$last][1]+strlen($m[$last][0]);
		
		if ($last==9) {
			$alias = $m[9][0];
			if (gladius_reserved(gladius_strtoupper($alias))) {
				if ($m[$last-1][1]!=-1) {
					$this->_error(_G__INVALID_ALIAS, $alias);
					$result = false;
					return;
				}
				// save the last field of the SELECT query and break matching
				$fields[] = array($m[1][0], '', $m[2][1]!=-1);
				$this->offset= $m[0][1] + strlen($m[0][0]);
				break;
			}
		} else
			$alias = $m[9][0];

		// alias unicity will later be checked
		$fields[] = array($m[1][0], $alias, $m[2][1]!=-1);
		
		if ($m[$last][0]!=',')
			break;
	} // WEND
	
	if (empty($fields)) {
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}
	
	// (2) get the source tables
	$from = array();
	if (!$this->_parse_from($from)) {
		$result = false;
		return;
	}
	
	// (3) initialize the source tables
	if (false ===($result = $this->_create_rs($from)))
		return;

	$result->in_subquery = $subquery;
	
	//(4) load the directly selected fields
	if (!$result->_load_fields($fields, $result->aliases) ||
	//(5) parse the WHERE clause and the interested fields
		!$result->_parse_where_expr()) {
		$result = false;
		return;
	}
	
//	debug_chain($result->where);
	
	$result->_aggregate = count($result->_calc_fields);
	
	if (!$subquery) {
		if ($result->_aggregate) {
			if (!$result->_parse_group_expr()) {
				$result = false;
				return;
			}
		} else {
			if (!$result->_parse_order_expr()) {
				$result = false;
				return;
			}
		}
	}
	
	$limits = $result->_parse_limit();
	if ($limits === false) {
		$this->_error(_G__INVALID_LIMIT);
		$result = false;
		return;
	}
	
	if (!$subquery) {
		if (!$this->_terminated()) {
			$result = false;
			return;
		}
	} else {
		// terminate the subquery
		if (preg_match('/\\s*\\)\\s*/A', $this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->offset = strlen($m[0][0])+$m[0][1];
		} else {
			$this->_error(_G__INVALID_TERMINATION);
			$result = false;
			return;
		}
	}
	
	//dicriminate between where columns and other columns
	foreach($result->allfields as $fo) {
		$col =& $this->_pick_column($fo->table, $fo->name);	// cache all fields
		if (!isset($col)) {
			$result = false;
			return;
		}
		if (!in_array($fo->name, $result->where_fields))
			$result->iteration_arg[] = $fo->name;	// index the non-where fields
	}

	$result->initiate();
	
	// setup value decoders
	global $GLADIUS_CUSTOM_DATA;
	$result->keys = array();
	$i = 0;
	foreach ($result->fields as $fo) {
		if (in_array($fo->type, $GLADIUS_CUSTOM_DATA)) {
			if (($result->fetch_mode == GLADIUS_FETCH_NUM) || ($result->fetch_mode == GLADIUS_FETCH_BOTH))
				$result->keys[$fo->name] = $i++;
			else
				$result->keys[$fo->name] = $result->fields[$fo->name]->alias;
		}
	}
	
	// if there's something to order, then get the full result
	if (!empty($result->order_way)) {
		include_once GLADIUS_DIR.'gladius_order.php';

		// iteration argument will contain fields not updated by WHERE evaluation + ordering fields not in the selected list
		$result->iteration_arg = array_merge($result->iteration_arg, $result->order_fields);
		// perform iteration without limits (we have to sort the data and then crop the selected)
		// code should be optimized just to consider order and where fields and finally add the other data too
		$result->limit_from = 0;
		$result->limit_length = 0;

		// create a rowset with ordering fields too
		while ($result->_iterate('_order_fetch')) { }
		
		if ($result->row_count==0) {
			$this->_success(0);
			return;
		}
		
		// calculate the aggregate fields
		$result->_calculate_fields();

		// apply ordering
		rowsort($result);
		
		$result->limit_from = $limits[0];
		$result->limit_length = $limits[1];

		// fix the resultset
		$result->recompile_rowset();

		unset($result->order_way);
		unset($result->order_ids);
		$open = true;
	} else {
		$result->limit_from = $limits[0];
		$result->limit_length = $limits[1];
		
		// if there are no aggregate functions proceed as normal
		if (!$result->_aggregate) {
			$result->_iterate('_fetch_row');
			$open = $result->EOF;
		} else {
			// calculate aggregate fileds if there are records
			if ($result->RecordCount())
				$result->_calculate_fields();
			$open = true;
		}
	}

	if (!$subquery)
		$this->_success($result->row_count, 'row', $open);
?>
