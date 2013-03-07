<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# DELETE command interpretation
#
	if (!preg_match('/\\s*from\\s*(\\w+)/iA', $sql, $m, PREG_OFFSET_CAPTURE, $m[1][1]+6)) {
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}
	$table = gladius_strtoupper($m[1][0]);
	$this->offset = $m[1][1]+strlen($table)+1;
	$from = array( array($table, '') );

	$schema =& $this->_pick_schema($table);
	if ($schema === null) {
		$result = false;
		return;
	}

	if (false ===($result = $this->_create_rs($from)))
		return;

	if (!$result->_parse_where_expr()) {
		$result = false;
		return;
	}

	if (!$result->_parse_limit()) {
		$this->_error(_G__INVALID_LIMIT);
		$result = false;
		return;
	}

	$result->initiate();	// can be optimized
	
	if (empty($result->where_fields)) {
		if ($result->limit_length != 0)
			$span = $result->limit_length;
		else
			$span = count($schema['RECORD_IDS']);
		$result->iteration_arg = array( array($result->limit_from, $span) );
	} else {
		foreach($result->where_fields as $field) {
			$fo =& $result->allfields[$field];
			$col =& $this->_pick_column($fo->table, $fo->name);
			if (!isset($col)) {
				$result = false;
				return;
			}
		}

		while ($result->_iterate('_delete_row')) { }
		
		if (empty($result->iteration_arg)) {

			$this->_success(0);
			$result = true;
			return;
		}
	}

	$fields = array_keys($schema['FIELD_DEF']);
	foreach ($fields as $field) {
		$column =& $this->_pick_column($table, $field);
		if ($column === null) {
			$result = false;
			return;
		}

		$delta = 0;
		foreach ($result->iteration_arg as $range) {
			array_splice($column, $range[0]-$delta, $range[1]);
			$delta += $range[1];
		}

		$fo =& $schema['FIELD_DEF'][$field];
		if ($fo->indexed)
			$result->_remove_order($fo, $result->iteration_arg);

		if (!$this->_save($table.'.'.$field, $column)) {
			$this->_error(_G__WRITE_FAILURE, $table.'.'.$field);
			$result = false;
			return;
		}

	}

	$delta = 0;
	foreach ($result->iteration_arg as $range) {
		array_splice($schema['RECORD_IDS'], $range[0] - $delta, $range[1]);
		$delta += $range[1];
	}

	if (!$this->_save($table, $schema)) {
		$this->_error(_G__WRITE_FAILURE, $table);
		$result = false;
		return;
	}

	$this->_success($delta);
	$result = true;
?>
