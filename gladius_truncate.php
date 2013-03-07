<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius  Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# TRUNCATE command interpretation
#
	$this->offset = $m[1][1]+8;
	if (!preg_match('/\\s*(\\w+)/', $sql, $m, 0, $this->offset)) {
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}
	$table = gladius_strtoupper($m[1]);
	$schema =& $this->_pick_schema($table);
	if (!isset($schema)) {
		$result = false;
		return;
	}
	
	$schema =& $this->schemas[$table];
	$delta = count($schema['RECORD_IDS']);
	$schema['RECORD_IDS'] = array();
	$schema['TOP_INSERT_ID'] = 0;
	if (!$this->_save($table, $schema)) {
		$this->_error(_G__WRITE_FAILURE, $table);
		$result = false;
		return;
	}

	$this->_create_table($table, $schema);
	
	$this->_success($delta);
	$result = true;
?>
