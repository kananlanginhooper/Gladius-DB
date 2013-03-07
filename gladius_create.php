<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# CREATE command interpretation
#

if (!function_exists('_data_type')) {
	function _data_type($type, &$fo) {
		$type = gladius_strtolower($type);

		switch ($type) {
			case 'char':
			case 'varchar':
				$fo->type = 'varchar';
				$fo->numeric = false;
				return true;
			case 'longtext':
				$fo->type = 'longtext';
				$fo->numeric = false;
				return true;
			case 'text':
				$fo->type = 'text';
				$fo->numeric = false;
				return true;
			case 'int':		// not in SQL92 standard
			case 'integer':
			case 'smallint':
			case 'bigint':
			case 'tinyint':	// not in SQL92 standard
				$fo->type = 'integer';
				$fo->numeric = true;
				return true;
			case 'numeric':
			case 'decimal':
			case 'float':
			case 'real':
			case 'double':	// not in SQL92 standard
//			case 'double precision': // not yet implemented
				$fo->type = 'double';
				$fo->numeric = true;
				return true;
			case 'date':
			case 'time':
			case 'datetime': // not in SQL92 standard
			case 'timestamp':
				$fo->type = $type;
				$fo->numeric = true;
			return true;
		}
		return false;
	}
}

	$this->offset = $m[1][1]+6;
	if (!preg_match('/\\s+table\\s+(\\w+)(\\s*\\(\\s*)/iA', $sql, $m,
			PREG_OFFSET_CAPTURE, $this->offset)) {
			if (preg_match('/\\s+database\\s+(\\w+)/iA', $sql, $m,
				PREG_OFFSET_CAPTURE, $this->offset)) {
					$database = gladius_strtolower($m[1][0]);
					$this->offset = $m[1][1]+strlen($database);
					include_once GLADIUS_DIR.'gladius_create_database.php';
					$result = _create_database($this, $database);
					return;
				}
			$this->_error(_G__MALFORMED_SQL);
			$result = false;
			return;
	}
	if (!isset($this->database)) {
		$this->_error(_G__INVALID_DB);
		$result = false;
		return;
	}

	$table = gladius_strtoupper($m[1][0]);

	if (in_array($table, $this->index['TABLES'])) {
		$this->_error(_G__TABLE_EXISTS, $table);
		$result = false;
		return;
	}

	$this->offset = $m[2][1] + strlen($m[2][0]);
	
	$n = null;
	$ev = new Gladius_Eval($this, $n, $n, $n);

	$fields = array();
	while (preg_match(
		'/\\s*('.
		'(primary\\s+key\\s*\\(\\s*(\\w+)\\s*\\))'.	// primary key definition
		'|('._G__UNIQUE.')'.
		// field name and type with eventual field size definition and auto_increment flag
		'|((\\w+)\\s+(\\w+)\\s*'.
		'((\\(\\s*(\\d+)\\s*\\))|(\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)))?'. //the maximum length or display length or prec.,scale couple
		'\\s*(\\w+)?)'. // the ending word
		')/iA', $sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {

//		{echo'<pre>';var_dump($m);echo'</pre><hr>';}

		$this->offset = $m[0][1]+strlen($m[0][0]);
		
		if (preg_match('/primary/iA', $m[2][0])) {
			if (isset($primary)) {
				$this->_error(_G__DUPLICATE_PRIMARY, $primary);
				$result = false;
				return;
			}

			// get the primary field
			$primary = $m[3][0];
			if (!isset($fields[$primary])) {
				$this->_error(_G__FIELD_NOT_FOUND, $primary);
				$result = false;
				return;
			}

			if (!$fields[$primary]->numeric) {
				$this->_error(_G__PRIMARY_NOT_NUMERIC, $primary, $fields[$primary]->type);
				$result = false;
				return;
			}

			$fields[$primary]->primary_key = true;
			$fields[$primary]->auto_increment = true; // this is implicit
			$fields[$primary]->unique_key = true; // this is implicit

			if (!$this->_close_ok($ok)) {
				$result = false;
				return;
			}

		continue;
	}
	elseif (preg_match('/'._G__UNIQUE.'/iA', $m[4][0], $unique_raw)) {
		$u_fields = explode(',', $unique_raw[1]);
		foreach($u_fields as $field) {
			$field = trim($field);
			if (!isset($fields[$field])) {
				$this->_error(_G__FIELD_NOT_FOUND, $field);
				$result = false;
				return;
			}
			$fields[$field]->unique_key = true;
		}

		$this->offset = $unique_raw[0][1]+strlen($unique_raw[0][0]);

		if (!$this->_close_ok($ok)) {
			$result = false;
			return;
		}

		continue;
	}

		// check for duplicate field definitions
		$fname = $m[7][0];
		foreach (array_keys($fields) as $fn) {
			if (gladius_strtolower($fn) == gladius_strtolower($fname)) {
				$this->_error(_G__DUPLICATE_FIELD, $fname, $fn);
				$result = false;
				return;
			}
		}

	// create the new field object
	$fo = new Gladius_Field;
	$fo->name = $fname;
	$fo->alias = $fname;
	$fo->table = $table;
	$fo->has_default = false;
//	$fo->default_value = null;	// might be enabled in future
	$fo->not_null = false;
	$fo->primary_key = false;
	$fo->unique_key = false;
	$fo->auto_increment = false;
	$fo->indexed = false;
	$fo->ordered = array();	// pre-ordered indices, if present

	if (!_data_type($m[8][0], $fo)) {
		$this->_error(_G__INVALID_TYPE, $m[8][0]);
		$result = false;
		return;
	}

	// skip options parsing for a very short DLD
	if (!isset($m[11])) {
		if (!$this->_close_ok($ok)) {
			$result = false;
			return;
		}

		$fields[$fo->name] = $fo;
		continue;
	}

	// check for the maximum length parameter
	if (strlen($m[11][0])) {
		$max_val = (int)$m[11][0];
		if ($max_val<1) {
			$this->_error(_G__INVALID_MAX_LENGTH, $max_val);
			$result = false;
			return;
		} else {
			if (!$fo->numeric && ($max_val>65535)) {
				$this->_error(_G__INVALID_MAX_LENGTH, $max_val);
				$result = false;
				return;
			}
			$fo->max_length = $max_val;
		}
	} else if (strlen($m[13][0])) {
		if ($fo->type !== 'double') {
			$this->_error(_G__FP_EXPECTED, $fo->name);
			$result = false;
			return;
		}
		$fo->precision = (int)$m[13][0];
		$fo->scale = (int)$m[14][0];
		if (!$fo->precision || !$fo->scale) {
			$this->_error(_G__INVALID_PS);
			$result = false;
			return;
		}
	}

	// check the field OPTIONS
	if (isset($m[15])) {
		$word = $m[15][0];
		do {
			switch (gladius_strtoupper($word)) {
				case 'NOT':
					if (!preg_match('/\\s+NULL/iA', $sql,$temp, PREG_OFFSET_CAPTURE, $this->offset)) {
						$this->_error(_G__MALFORMED_SQL);
						$result = false;
						return;
					}
					$this->offset=$temp[0][1]+strlen($temp[0][0]);
					$fo->not_null = true;
					break;
				case 'AUTO_INC':
				case 'AUTO_INCREMENT':
					$fo->auto_increment = true;
					break;
				case 'UNIQUE':
					$fo->unique_key = true;
					break;
				case 'DEFAULT':
					if (false === ($def = $ev->_get_immediate())) {
						$this->_error(_G__INVALID_DEFAULT, $fo->name);
						$result = false;
						return;
					}
					$fo->default_value = $def->evaluate();
					$fo->has_default = true;
					break;
			}
//			echo substr($sql, $this->offset).'<hr>';
			if (preg_match('/\\s*(\\w+)/A',$sql,$temp,PREG_OFFSET_CAPTURE,$this->offset)) {
				$this->offset=$temp[0][1]+strlen($temp[0][0]);
				$word = $temp[1][0];
			} else $word = null;
		} while ($word!==null);
	}

	if (!$this->_close_ok($ok)) {
		$result = false;
		return;
	}

	$fields[$fo->name] = $fo;
	} // loop for each field definition

	if (empty($fields)) {
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}


	if (!isset($ok)) { // a valid termination is missing
		$this->_error(_G__MISS_CLS);
		$result = false;
		return;
	}

	if (!$this->_terminated()) {
		$result = false;
		return;
	}

	$schema = array(
					'FIELD_DEF' => $fields,		// complete definitions for fields
					'TOP_INSERT_ID' => 0,
					'RECORD_IDS' => array(),	// always progressive record ID for internal or PRIMARY use
					// Gladius version, added to each table schema
					'GV' => (int)((GLADIUS_HI_VER << 16) | (GLADIUS_LO_VER))
				);
	if (!$this->_save($table, $schema)) {
		$this->_error(_G__WRITE_FAILURE, $table);
		$result = false;
		return;
	}

	$this->_build_index();

	$this->index['TABLES'] = array_merge($this->index['TABLES'], array( $table ));

	if (!$this->_save('gladius.db.master', $this->index)) {
		$this->_error(_G__WRITE_FAILURE, 'gladius.db.master');
		$result = false;
		return;
	}

	if (!$this->_create_table($table, $schema)) {
		$result = false;
		return;
	}

	$this->_success(1, 'table');
	$result = true;
?>
