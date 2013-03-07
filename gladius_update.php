<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# UPDATE command interpretation
#
	if (!preg_match('/\\s*(\\w+)\\s+set(\\s)/iA',
					$sql, $m, PREG_OFFSET_CAPTURE, $m[1][1]+6)) {
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}

	$table = gladius_strtoupper($m[1][0]);
	$from = array( array($table, '') );
	$this->offset = $m[2][1]+1;
	
	$result = $this->_create_rs($from);
	if ($result === false)	return;
		
	$result->ops = array();
	while (preg_match('/\\s*(\\w+)\\s*=\\s*(.)/iA',
					  $sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {

		$field = $m[1][0];

		if (!isset($this->schemas[$table]['FIELD_DEF'][$field])) {
			$this->_error(_G__FIELD_NOT_FOUND, $field);
			$result = false;
			return;
		}
		$result->allfields[$field] =& $this->schemas[$table]['FIELD_DEF'][$field];
		$col =& $this->_pick_column($table, $field);
		if (!isset($col)) {
			$result = false;
			return;
		}

		$this->offset = $m[2][1];
		
		$GLOBALS['flagg'] = true;
		
		if (false === ($result->ops[$field] = $result->ev->_get_hand())) {
			if ($this->errno == _G__SUCCESS)
				$this->_error(_G__MALFORMED_SQL);
			$result = false;
			return;
		}
		
		$GLOBALS['flagg']= false;

		preg_match('/\\s*(.|$)/A', $sql, $m, PREG_OFFSET_CAPTURE, $this->offset);

		$this->offset = $m[0][1];
		if ($m[1][0] != ',')
			break;
		$this->offset += strlen($m[0][0]);
	}

	if (empty($result->ops)) {
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}

	if (!$result->_parse_where_expr()) {
		$result = false;
		return;
	}

	if (!$result->_parse_limit()) {
		$this->_error(_G__INVALID_LIMIT);
		$result = false;
		return;
	}

	$result->initiate();

	foreach($result->where_fields as $field) {
		$fo =& $result->allfields[$field];
		$col =& $this->_pick_column($fo->table, $field);
		if (!isset($col)) {
			$result = false;
			return;
		}
	}
	
	$result->updated = array();

	while ($result->_iterate('_update_row')) { }
	if ($this->errno != _G__SUCCESS) {
		$result = false;
		return;
	}

		
	if (empty($result->iteration_arg)) {
		$this->_success(0);
		$result = true;
		return;
	}
	
	$fields = array_keys($result->ops);
	foreach ($fields as $field) {
		$column =& $this->pool[$table][$field];
		$delta = 0;

		$fo =& $this->schemas[$table]['FIELD_DEF'][$field];
		if ($fo->indexed)
			$result->_remove_order($fo, $result->iteration_arg);

		foreach ($result->iteration_arg as $range) {
			array_splice($column, $range[0], $range[1],
						 array_slice($result->updated[$field], $delta, $range[1])
			);
			$delta += $range[1];
			if ($fo->indexed) {
				for ($i=0;$i<$range[1];++$i) {
					$this->_update_order($fo, $column, $result->updated[$field][$range[0]+$i]);
				}
			}
		}
		if (!$this->_save($table.'.'.$field, $column)) {
			$this->_error(_G__WRITE_FAILURE, $table.'.'.$field);
			$result = false;
			return;
		}

	}

	if (!$this->_save($table, $this->schemas[$table])) {
		$this->_error(_G__WRITE_FAILURE, $table);
		$result = false;
		return;
	}

	$this->_success($delta);
	$result = true;
?>
