<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# INSERT command interpretation
#
	if (!preg_match('/\\s+into\\s+(\\w+)\\s*(\\([\\w\\s,]+\\))?\\s*values\\s*\\((.)/iA',
		$sql, $m, PREG_OFFSET_CAPTURE, $m[1][1]+6)) {
//					echo substr($sql, $m[1][1]+6).'<br>';
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}

	$table = gladius_strtoupper($m[1][0]);

	if (null === ($schema =& $this->_pick_schema($table))) {
		$result = false;
		return;
	}

	$smart_insert = empty($m[2][0]);
	if ($smart_insert)
		$fields = $schema['FIELD_DEF'];
	else {
		$fields_list = preg_replace('/\\s/', '', substr($m[2][0], 1, strlen($m[2][0])-2));

		$selected = explode(',', $fields_list);
		foreach($selected as $field) {
			if (!isset($schema['FIELD_DEF'][$field])) {
				$this->_error(_G__FIELD_NOT_FOUND, $field);
				$result = false;
				return;
			}
			$fields[$field] =& $schema['FIELD_DEF'][$field];
		}
	}

	$n = null;
	$ev = new Gladius_Eval($this, $n, $n, $n);
	$this->offset = $m[3][1];
	$record = array();
	reset($fields);
	
	do {
		$fo =& $fields[key($fields)];
		if (false === ($record[$fo->name] = $ev->_get_immediate())) {
			$this->_error(_G__MALFORMED_SQL);
			$result = false;
			return;
		}
		if (!preg_match('/\\s*(,|\\))/A', $sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->_error(_G__MALFORMED_SQL);
			$result = false;
			return;
		}

		$this->offset = $m[1][1]+1;

		switch ($m[1][0]) {
			case ')':
				break 2;
			case ',':
			break;
			default:
				$this->_error(_G__MALFORMED_SQL);
				$result = false;
				return;
		}

		if (next($fields)===false) {
			$this->_error(_G__TOO_MANY_FIELDS);
			$result = false;
			return;
		}
	} while (true); // loop for each field value

	if (!$smart_insert and (count($record) != count($fields))) {
		$this->_error(_G__NOT_ENOUGH_FIELDS);
		$result = false;
		return;
	}

	$fdef =& $schema['FIELD_DEF'];
	$required = array_keys($fdef);
	$edited = array();
	
	// fix the primary key and the insert_id
	$this->insert_id = null;
	$top_iid = $schema['TOP_INSERT_ID'];
	foreach($required as $field) {
		$fo =& $fdef[$field];
		if (!$fo->primary_key) {
			if (!isset($record[$field]))
				$record[$field] = null;
			continue;
		}

		if (isset($record[$field])) {
			if (!$ev->_retrieve_value($record[$field]->evaluate(), $fo, $val)) {
				$result = false;
				return;
			}
			
			$column =& $this->_pick_column($table, $fo->name);
			if (!isset($column)) {
				$result = false;
				return;
			}

			if (in_array($val, $column)) {
				$this->_error(_G__DUPLICATE_KEY, $val);
				$result = false;
				return;
			}
			// if the new id is greater than the current one, overwrite it
			if ($val > $schema['TOP_INSERT_ID']) {
				$schema['TOP_INSERT_ID'] = $val;
				$top_iid = $val;
			}
		} else {
			$val = $schema['TOP_INSERT_ID']+1;
			$top_iid = $val;
		}

		$this->insert_id = $val;
		$edited[$field] = $val;
		
		// only one primary key, but the loop continues to set the null fields
	} // loop for next field
	
	if (!isset($this->insert_id)) {
		$top_iid = $schema['TOP_INSERT_ID']+1;
		$this->insert_id = $top_iid;
	}

	// now process the normal fields (we have a valid insert_id)
	// $data is the decoded data while $record is the array of not-yet-computed fields
	foreach ($record as $field => $hand) {
		$fo =& $fdef[$field];
		if ($fo->primary_key) continue;
		$column =& $this->_pick_column($table, $fo->name);
		if (!isset($column)) {
			$result = false;
			return;
		}

		if (!isset($hand)) {
			if ($fo->auto_increment) {
				$data = (int)end($column) + 1;
			} else if (!$fo->has_default && $fo->not_null) {
				$this->_error(_G__FIELD_NOT_NULL, $field);
				$result = false;
				return;
			} else {
				if (in_array($fo->type, $GLOBALS['GLADIUS_CUSTOM_DATA'])) {
					$decfunc = 'gladius_'.$fo->type.'_dec';
					if (!$decfunc(($fo->has_default ? $fo->default_value : null), $data, $this, $fo)) {
						$result = false;
						return;
					}
				} else
					$data = ($fo->has_default ? $fo->default_value : null);
			}
		} else if (!$ev->_retrieve_value($hand->evaluate(), $fo, $data)) {
			$result = false;
			return;
		}

		if ($fo->unique_key) {
			if (in_array($data, $column)) {
				$this->_error(_G__UNIQUE_VIOLATED, $fo->name, $data);
				$result = false;
				return;
			}
		}
		
		$edited[$field] = $data;

	} // loop for next field
	unset($records);
	
	foreach ($edited as $field => $val) {
		$fo =& $fdef[$field];
		$column =& $this->_pick_column($table, $fo->name);
		if (!isset($column)) {
			$result = false;
			return;
		}
		$column[] = $val;
		if ($fo->indexed)
			$this->_update_order($fo, $column, $val);
		// save data to file
		if (!$this->_save($table.'.'.$field, $column)) {
			$this->_error(_G__WRITE_FAILURE, $table.'.'.$field);
			$result = false;
			return;
		}
	}

	$schema['RECORD_IDS'][] = $this->insert_id;
	$schema['TOP_INSERT_ID'] = $top_iid;
	
	if (!$this->_save($table, $schema)) {
		$this->_error(_G__WRITE_FAILURE, $table);
		$result = false;
		return;
	}
	
	$this->_success(1);
	$result = true;
?>
