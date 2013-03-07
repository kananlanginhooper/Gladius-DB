<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# DESCRIBE statement implementation
#

	if (!isset($this->database)) {
		$this->_error(_G__INVALID_DB);
		$result = false;
		return;
	}

	$this->offset=$m[0][1]+strlen($m[0][0]);
	if (!preg_match('/\\s*(\\w+)/iA', $sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}
	$this->offset=$m[0][1]+strlen($m[0][0]);

	$table = gladius_strtoupper($m[1][0]);

	if (false ===($result = $this->_create_rs(array())))
		return;

	$schema =& $this->_pick_schema($table);
	if (!isset($schema)) {
		$result = false;
		return;
	}

	foreach ($schema as $fo) {
		$type = $fo->type;
		if (isset($fo->max_length))
			$type.='('.$fo->max_length.')';
		if ($fo->primary_key)
			$key = 'PRI';
		elseif ($fo->unique_key)
			$key = 'UNI';
		else $key = '';
		if ($fo->auto_increment)
			$extra = 'auto_increment';
		else $extra = '';
		switch ($result->fetch_mode) {
			case GLADIUS_FETCH_BOTH:
				$record = array($fo->name, $type, !$fo->not_null, $key, $fo->default_value, $extra);
				$record['field'] =& $record[0];
				$record['type'] =& $record[1];
				$record['null'] =& $record[2];
				$record['key'] =& $record[3];
				$record['default'] =& $record[4];
				$record['extra'] =& $record[5];
			break;
			case GLADIUS_FETCH_NUM:
				$record = array($fo->name, $type, !$fo->not_null, $key, $fo->default_value, $extra);
			break;
			default:
			case GLADIUS_FETCH_DEFAULT:
			case GLADIUS_FETCH_ASSOC:
				$record = array('field' => $fo->name, 'type' => $type, 'null' => !$fo->not_null, 'key' => $key, 'default' => $fo->default_value, 'extra' => $extra);
		}
		$result->rowset[] = $record;
	}

	$result->EOR = true;
	$result->row_count = count($result->rowset);
	$result->aliases = array('field', 'type', 'null', 'key', 'default', 'extra');
	$result->EOF = ($result->row_count == 0);

	$this->_success($result->row_count);
?>
