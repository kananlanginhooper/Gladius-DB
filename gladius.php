<?php
## Gladius Database Engine
# @author legolas558
# @version 0.8.1 rev.196
# Licensed under GNU General Public License (GPL)
#
#
# Flatfile php database engine compliant with a subset of intermediate SQL92
#

if (strnatcmp(phpversion(), '4.3.3')<0)
	trigger_error('Gladius DB requires at least PHP v4.3.3');

define('GLADIUS_HI_VER', 0);
define('GLADIUS_LO_VER', 8);

define('GLADIUS_DIR', dirname(__FILE__).'/');

// fetch mode constants
define('GLADIUS_FETCH_DEFAULT',0);
define('GLADIUS_FETCH_NUM',1);
define('GLADIUS_FETCH_ASSOC',2);
define('GLADIUS_FETCH_BOTH',3);

global $GLADIUS_FETCH_FUNC;

$GLADIUS_FETCH_FUNC = array( '_fetch_assoc', '_fetch_num', '_fetch_assoc', '_fetch_both');

// error constants
define('_G__SUCCESS', 0);
define('_G__MALFORMED_SQL', 1);
define('_G__UNRECON_SQL', 2);
define('_G__TABLE_NOT_FOUND', 3);
define('_G__INVALID_MAX_LENGTH', 4);
define('_G__INVALID_TYPE', 5);
define('_G__INVALID_DEFAULT', 6);
define('_G__MISS_CLS', 7);
define('_G__FIELD_NOT_FOUND', 8);
define('_G__DUPLICATE_PRIMARY', 9);
define('_G__COMMA_EXPECTED', 10);
define('_G__FIELD_TYPE_MISMATCH', 11);
define('_G__FIELD_NOT_NULL', 12);
define('_G__EXCEED_MAX_LENGTH', 13);
define('_G__NOT_ENOUGH_FIELDS', 14);
define('_G__DUPLICATE_KEY', 15);
define('_G__TABLE_EXISTS', 16);
define('_G__INVALID_ALIAS', 17);
define('_G__AMBIGUOUS_FIELD', 18);
define('_G__TABLE_NOT_SELECTED', 19);
define('_G__NO_SOURCE', 20);
define('_G__CONVERSION_ERROR', 21);
define('_G__AMBIGUOUS_ALIAS', 22);
define('_G__ASTERISK_AGAIN', 23);
define('_G__INVALID_LIMIT', 24);
define('_G__UNEXPECTED_CLOSEP', 25);
define('_G__BOOL_OP_EXPECTED', 26);
define('_G__INVALID_BOOL_OP', 27);
define('_G__LHAND_EXPECTED', 28);
define('_G__RHAND_EXPECTED', 29);
define('_G__OPERATOR_EXPECTED', 30);
define('_G__DUPLICATE_SOURCE', 31);
define('_G__TYPE_MISMATCH', 32);
define('_G__UNKNOWN_BOOL_OP', 33);
define('_G__DB_NOT_FOUND', 34);
define('_G__DB_ALREADY_EXISTS', 35);
define('_G__REMOVE_ERROR', 36);
define('_G__UNIQUE_VIOLATED', 37);
define('_G__AND_EXPECTED', 38);
define('_G__SET_EXPECTED', 39);
define('_G__TOO_MANY_FIELDS_SUBQ', 40);
define('_G__EXPECTED_OPP', 41);
define('_G__CANNOT_CREATE_DB', 42);
define('_G__DUPLICATE_FIELD', 43);
define('_G__INVALID_SCALAR', 44);
define('_G__EMPTY_SCALAR', 45);
define('_G__INVALID_DB', 46);
define('_G__INVALID_DB_ROOT', 47);
define('_G__PRIMARY_NOT_NUMERIC', 49);
define('_G__TABLE_LOCK_FAILED', 48);
define('_G__WRITE_FAILURE', 49);
define('_G__INVALID_PS', 50);
define('_G__FP_EXPECTED', 51);
define('_G__INVALID_TERMINATION', 52);
define('_G__READ_FAILURE', 53);
define('_G__TOO_MANY_FIELDS', 54);
define('_G__CANNOT_SELECT_DB', 55);
define('_G__DB_ROOT_INVALID', 56);

global $GLADIUS_WARNINGS_AS_ERRORS;
//customizable constants & globals
define('_G__COMPRESSION_THRESHOLD', 8000);
$GLADIUS_WARNINGS_AS_ERRORS = false;
//end of customizable constants & globals

// other Gladius constants
define('_G__PHP_HEAD', '<'.'?php die;?'.'>');
define('_G__STRING', "('[^']*')+");
// FIELD REGEX HERE
define('_G__UNIQUE', 'unique\\s+\\(\\s*([\\w,\\s]+)\\)');
define('_G__DATE_FORMAT', "%Y-%m-%d");
define('_G__TIME_FORMAT', "%H:%M:%S");
define('_G__DATETIME_FORMAT', _G__DATE_FORMAT.' '._G__TIME_FORMAT);

global $GLADIUS_BOOL_OPS, $GLADIUS_ERRORS, $GLADIUS_OPS;

global $GLADIUS_RESERVED;

$GLADIUS_RESERVED = array('FROM', 'ORDER', 'GROUP', 'WHERE', 'LIMIT');

function gladius_reserved($word) {
	global $GLADIUS_RESERVED;
	return in_array(gladius_strtoupper($word), $GLADIUS_RESERVED);
}

$GLADIUS_ERRORS = array(
	_G__SUCCESS => '%s statement successfully executed, %d %s affected',
	_G__MALFORMED_SQL => 'SQL syntax error',
	_G__UNRECON_SQL => 'Unrecognized SQL statement "%s"',
	_G__TABLE_NOT_FOUND => 'Table "%s" not found',
	_G__READ_FAILURE => 'Could not read from file "%s"',
	_G__INVALID_MAX_LENGTH => 'Invalid value "%s" for maximum length',
	_G__INVALID_TYPE => 'Unsupported SQL data type "%s"',
	_G__MISS_CLS => 'Expected terminating \')\'',
	_G__FIELD_NOT_FOUND => 'Field "%s" not found in source tables',
	_G__INVALID_DEFAULT => 'Invalid default value specified for field "%s"',
	_G__DUPLICATE_PRIMARY => 'Primary key already defined for field "%s"',
	_G__FIELD_TYPE_MISMATCH => 'Type mismatch for field "%s", expected data of type %s',
	_G__FIELD_NOT_NULL => 'Field "%s" cannot be null',
	_G__EXCEED_MAX_LENGTH => 'Exceeded maximum length for field "%s" with value "%s"',
	_G__NOT_ENOUGH_FIELDS => 'Not enough fields to fill a record',
	_G__TOO_MANY_FIELDS => 'Too many fields',
	_G__DUPLICATE_KEY => 'Cannot insert record with key "%d", it already exists',
	_G__TABLE_EXISTS => 'Table "%s" already exists',
	_G__INVALID_ALIAS => 'Invalid alias "%s"',
	_G__AMBIGUOUS_ALIAS => 'Ambiguous alias "%s" defined',
	_G__AMBIGUOUS_FIELD => 'Field "%s" is ambiguously referenced',
	_G__DUPLICATE_FIELD => 'Cannot create field "%s" because field "%s" is already defined',
	_G__TABLE_NOT_SELECTED => 'Table "%s" was not selected',
	_G__NO_SOURCE => 'No source table specified',
	_G__CONVERSION_ERROR => '%s conversion error',
	_G__ASTERISK_AGAIN => 'Cannot use any field qualifer (*) again',
	_G__INVALID_LIMIT => 'Invalid limit boundaries specified',
	_G__UNEXPECTED_CLOSEP => 'Unexpected closing ")" found',
	_G__BOOL_OP_EXPECTED => 'Boolean operator expected',
	_G__INVALID_BOOL_OP => 'Invalid boolean operator "%s"',
	_G__LHAND_EXPECTED => 'Left hand operator expected',
	_G__RHAND_EXPECTED => 'Right hand operator expected',
	_G__OPERATOR_EXPECTED => 'Comparison operator expected',
	_G__DUPLICATE_SOURCE => 'Duplicate source "%s" in tables list',
	_G__TYPE_MISMATCH => 'Type mismatch, left hand is of type "%s" and right hand is of type "%s"',
	_G__UNKNOWN_BOOL_OP => 'Unknown boolean operator "%s"',
	_G__DB_NOT_FOUND => 'Database directory "%s" not found',
	_G__DB_ALREADY_EXISTS => 'Database directory "%s" already exists',
	_G__REMOVE_ERROR => 'Error while removing "%s"',
	_G__CANNOT_CREATE_DB => 'Cannot create database "%s"',
	_G__UNIQUE_VIOLATED => 'Unique constraint violated for field "%s" with value "%s"',
	_G__AND_EXPECTED => 'AND expected in BETWEEN expression',
	_G__SET_EXPECTED => 'Explicit values set or SELECT subquery expected',
	_G__TOO_MANY_FIELDS_SUBQ => 'Too many fields in subquery expression',
	_G__EXPECTED_OPP => 'Opening parenthesis "(" expected',
	_G__COMMA_EXPECTED => 'Comma character "," was expected',
	_G__INVALID_SCALAR => 'Resultset expected, but %s boolean result found',
	_G__EMPTY_SCALAR => 'Cannot retrieve scalar value from empty resultset',
	_G__INVALID_DB => 'No valid database selected',
	_G__CANNOT_SELECT_DB => 'Cannot select database "%s"',
	_G__INVALID_DB_ROOT => 'Invalid database root folder "%s"',
	_G__PRIMARY_NOT_NUMERIC => 'The primary key field "%s" must be numeric, but its type is %s',
	_G__INVALID_PS => 'The primary key field "%s" must be numeric, but its type is %s',
	_G__FP_EXPECTED => 'Field "%s" is not of floating-point type, therefore (precision,scale) cannot be specified',
	_G__INVALID_TERMINATION => 'Statement termination was expected',
	_G__TABLE_LOCK_FAILED => 'Locking of table "%s" failed',
	_G__WRITE_FAILURE => 'Write failure on file "%s"',
	_G__DB_ROOT_INVALID => 'Invalid database root "%s"',
);

$GLADIUS_OPS = array(
	'=' => 'equ',
	'>=' => 'geq',
	'>=' => 'leq',
	'<>' => 'nequ',
	'like' => 'like',
	'nlike' => 'nlike',
	'between' => 'between',
	'nbetween' => 'nbetween',
	);

$GLADIUS_BOOL_OPS = array(
	'or',
	'and',
	'nor',
	'nand'
	);

function gladius_unescape($s) {
	return str_replace("''", "'", $s);
}
	
// end of gladius_const.php
// begin gladius_types.php

global $GLADIUS_CUSTOM_DATA;

$GLADIUS_CUSTOM_DATA = array(
'datetime',
'date',
'time',
);

// custom data decoder/encoder functions
function gladius_datetime_enc($data, &$dest, &$conn, &$fo, $id) {
	if ($data === null) {
		$dest = null;
		return true;
	}

	if (!preg_match('/((\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)\\s+)?((\\d\\d):(\\d\\d)(:(\\d\\d))?)?/A',
					$data, $m))
		return false;

	if ($m[0] === '')
		return false;
	$dest = gmmktime((int)@$m[6], (int)@$m[7], (int)@$m[9], (int)@$m[3], (int)@$m[4], (int)@$m[2]);

	return true;
}

function gladius_time_enc($data, &$dest, &$conn, &$fo, $id) {
	return gladius_datetime_enc($data, $dest, $conn, $fo, $id);
}

function gladius_date_enc($data, &$dest, &$conn, &$fo, $id) {
	return gladius_datetime_enc($data, $dest, $conn, $fo, $id);
}

function gladius_datetime_dec($data, &$dest, &$conn, &$fo) {
	if ($data === null) {
		$dest = null;
		return true;
	}
	$dest = gmstrftime(_G__DATETIME_FORMAT, $data);
	return true;
}

function gladius_time_dec($data, &$dest, &$conn, &$fo) {
	if ($data === null) {
		$dest = null;
		return true;
	}
	$dest = gmstrftime(_G__TIME_FORMAT, $data);
	return true;
}

function gladius_date_dec($data, &$dest, &$conn, &$fo) {
	if ($data === null) {
		$dest = null;
		return true;
	}
	$dest = gmstrftime(_G__DATE_FORMAT, $data);
	return true;
}

function gladius__remove($path, $field, &$schema) {
	return @unlink($path.'.'.$field.'.php');
}

if (strtoupper('abcdefghijklmnopqrstuvwxyz')!='ABCDEFGHIJKLMNOPQRSTUVWXYZ') {

	// used for an ASCII strtoupper()
	function _gladius_strtoupper_cb($m) { return chr(ord($m[0])-32); }
	function gladius_strtoupper($s) {
		if (!isset($s[0])) return '';
		return preg_replace_callback('/[a-z]/', '_gladius_strtoupper_cb', $s);
	}
} else {
	function gladius_strtoupper($s) {return strtoupper($s);}
}

if (strtolower('ABCDEFGHIJKLMNOPQRSTUVWXYZ')!='abcdefghijklmnopqrstuvwxyz') {

	// used for an ASCII strtolower()
	function _gladius_strtolower_cb($m) { return chr(ord($m[0])-32); }
	function gladius_strtolower($s) {
		if (!isset($s[0])) return '';
		return preg_replace_callback('/[a-z]/', '_gladius_strtolower_cb', $s);
	}
} else {
	function gladius_strtolower($s) {return strtolower($s);}
}
// end of gladius_types.php

//FIXME: deprecated
function gladius_sql_count(&$values) {
	$result = $c = count($values);
	foreach($values as $v) {
		if (!isset($v))
			--$result;
	}
	for($i=0;$i<$c;++$i) {
		$values[$i] = $result;
	}
	
	return true;
}

function gladius_sql_avg(&$values) {
	$c = count($values);
	if (!$c) return true;
	$avg=0;
	foreach($values as $v) {
		if (!isset($v))
			--$c;
		$avg+=$v;
	}
	$avg/=$c;
	for($i=0;$i<$c;++$i) {
		$values[$i] = $avg;
	}
	return true;
}

function gladius_sql_sum(&$values) {
	$c = count($values);
	if (!$c) return true;
	$sum=array_sum($values);
	for($i=0;$i<$c;++$i) {
		$values[$i] = $sum;
	}
	return true;
}

function gladius_sql_max(&$values) {
	$c = count($values);
	if (!$c) return true;
	$m = max($values);
	for($i=0;$i<$c;++$i) {
		$values[$i] = $m;
	}
	return true;
}

function gladius_sql_min(&$values) {
	$c = count($values);
	if (!$c) return true;
	$m = min($values);
	for($i=0;$i<$c;++$i) {
		$values[$i] = $m;
	}
	return true;
}


class Gladius_Field {
	var $name;
	var $alias;
	var $table;
	var $has_default;
	var $default;
	var $not_null;
	var $primary_key;
	var $unique_key;
	var $auto_increment;	// ought to be 'serial'
	var $indexed;
	var $order;
	var $max_length;
	var $precision;
	var $scale;
}

class begin_expr extends Gladius_Core {

var $bool_op;
var $subexpr;
var $next;

	function begin_expr(&$obj) {
		$this->_link($obj);
	}

	## used in chains
	function evaluate() {
		return $this->test();
	}

	function _evaluate() {
//		echo "evaluating begin_expr:<br>";
		if (!isset($this->subexpr))
			return true;
//		debug_chain($this->subexpr);
		return $this->subexpr->test();
	}

	function test() {
/*		global $flagg;
		if (@$flagg) {
			echo ' (<b>'.get_class($this).'</b> [';
		}	*/

		$self = $this->_evaluate();
/*		global $flagg;
		if (@$flagg)
			echo ']= '.($self ? 'true' : 'false').' )'; */
			


		if (!isset($this->next)) {
/*			global $flagg;
			if (@$flagg)
				echo '<br />'; */
			return $self;
		}
		
		//FIXME: parenthesis previous criteria not respected!!
		if (!isset($this->bool_op))
			return $this->next->test();

/*		global $flagg;
		if (@$flagg)
			echo ' '.$this->bool_op;	*/
			
		switch ($this->bool_op) {
			case 'or':
				return ($self || $this->next->test());
			case 'and':
				return ($self && $this->next->test());
			case 'nor':
				return ($self || !$this->next->test());
			case 'nand':
				return ($self && !$this->next->test());
		}
		$this->_error(_G__UNKNOWN_BOOL_OP, $this->bool_op);
		return null;
	}
}


// base class with error handling features
class Gladius_Core {
	var $errstr;
	var $errno = _G__SUCCESS;
	var $sql;
	var $offset;
	var $_command;
	
	function _link(&$obj) {
		$this->_command =& $obj->_command;
		$this->errno =& $obj->errno;
		$this->errstr =& $obj->errstr;
		$this->offset =& $obj->offset;
		$this->sql =& $obj->sql;
	}

	function _log($msg) {
	// enable the below line to show errors
//		echo '<p style="font-family: lucida, tahoma; font-weight:bold; color: red;">[GLADIUS DB] '.$msg.'</p>';
		trigger_error($msg, E_USER_NOTICE);
	}

	function _error_set($args) {
		$errno = $args[0];
	
		global $GLADIUS_ERRORS;

		if (count($args)>1) {
			$args = array_slice($args, 1);
			$this->errstr=call_user_func_array('sprintf', array_merge(array($GLADIUS_ERRORS[$errno]), $args));
		} else
			$this->errstr=$GLADIUS_ERRORS[$errno];

		if ($this->_command)
			$this->errstr .= ' in command '.$this->_command;
		$this->errstr.=' at offset '.$this->offset.' /';
		$b = $this->offset + 10;
		if (isset($this->sql)) {	// there must be some bug somewhere
			if ($b > strlen($this->sql))
				$b = strlen($this->sql);
			$this->errstr .= substr($this->sql, $this->offset, $b-$this->offset).'/';
		}
	}
	
	function _error($errno) {
	/*		if ($this->errno !== _G__SUCCESS)
			trigger_error('Error collision: '.$this->errstr);	*/
	
		$this->errno = $errno;
		
		$this->_error_set(func_get_args());
		if ($dbg = @debug_backtrace())
			$dbginfo = 'line '.$dbg[0]['line'].', file '.basename($dbg[0]['file']).' ';
		else $dbginfo = '';
		unset($this->sql);
		$this->_log($dbginfo.$this->errstr);
	}
	
	function _warning($errno) {
		$this->_error_set(func_get_args());
/*		if ($dbg = @debug_backtrace())
			$dbginfo = 'line '.$dbg[0]['line'].', file '.basename($dbg[0]['file']).' ';
		else $dbginfo = '';
		unset($this->sql); */
		$this->_log(/*@$dbginfo.*/ $this->errstr);
	}

} // class Gladius_Core

class Gladius_Eval extends Gladius_Core {

var $sources;
var $where_fields;
var $allfields;	// all fields available

	function Gladius_Eval(&$conn, &$sources, &$where_fields, &$allfields) {
		$this->_link($conn);
		$this->conn =& $conn;
		$this->sources =& $sources;
		$this->where_fields =& $where_fields;
		$this->allfields =& $allfields;
	}

	function &_new_field(&$fo) {
		if ($fo->numeric)
			$f = new nfield($fo, $this->conn);
		else
			$f = new sfield($fo, $this->conn);
		$vf =& $f;
		return $vf;
	}
	
	function &_get_field($table_name, $name) {
		if ($table_name!=='') {
			if (!isset($this->sources[$table_name])) {
				$this->_error(_G__TABLE_NOT_FOUND, $table_name);
				$fo = false;
				$vf=&$fo;
				return $vf;
			}
			if (!isset($this->sources[$table_name]['FIELD_DEF'][$name])) {
				$this->_error(_G__FIELD_NOT_FOUND, $name);
				$fo = false;
				$vf =& $fo;
				return $vf;
			}
			$fo =& $this->sources[$table_name]['FIELD_DEF'][$name];
		} else {
			foreach($this->sources as $table_name => $table) {
				foreach ($table['FIELD_DEF'] as $k => $field) {
					if (($field->name === $name) || ($field->alias === $name) ) {
						$fo =& $this->sources[$table_name]['FIELD_DEF'][$k];
						break 2;
					}
				}
			}
			if (!isset($fo)) {
				$this->_error(_G__FIELD_NOT_FOUND, $name);
				$fo = false;
				$vf =& $fo;
				return $vf;
			}
		}

		//$fo->ref = true;
		if (!in_array($name, $this->where_fields, true))
			$this->where_fields[] = $name;

		if (!isset($this->allfields[$name]))
			$this->allfields[$name] =& $fo;

		return $fo;
	}
	
	function &_get_hand() {
		$hand = $this->_get_immediate();
		if ($hand !== false) {
			$vf =& $hand;
			return $vf;
		}
		if (preg_match('/\\s*(\\w+)\\.(\\w+)/A', $this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->offset = $m[2][1]+strlen($m[2][0]);
			$fo =& $this->_get_field(gladius_strtoupper($m[1][0]), $m[2][0]); /* the table and the field */
			if ($fo !== false)
				$hand =& $this->_new_field($fo);
			else
				$hand = false;
		} else if (preg_match('/\\s*(\\w+)/A', $this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->offset = $m[0][1]+strlen($m[0][0]);
			$field = $m[1][0];
			if (gladius_strtoupper($field) === 'NULL') {
				$hand = new nvalue( NULL, $this);
				return $hand;
			}
			$fo =& $this->_get_field('', $field);
			if ($fo !== false)
				$hand =& $this->_new_field($fo);
			else
				$hand = false;
		} else
			$hand = false;
		$vf =& $hand;
		return $vf;
	}

	function _get_immediate() {
		if (preg_match('/\\s*'._G__STRING.'/A', $this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->offset = $m[0][1]+strlen($m[0][0]);
			return new varchar_value((string)gladius_unescape(substr(trim($m[0][0]),1,-1)), $this);
		}
		if (preg_match('/\\s*([\\+-]?\\d+\\.\\d+)/A', $this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->offset = $m[0][1]+strlen($m[0][0]);
			return new double_value((double)$m[1][0], $this);
		}
		if (preg_match('/\\s*([\\+-]?\\d+)/A', $this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->offset = $m[0][1]+strlen($m[0][0]);
			return new integer_value((int)$m[1][0], $this);
		}
		if (preg_match('/\\s*(\\w+)\\s*\\(/A', $this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->offset = $m[0][1]+strlen($m[0][0]);
			$subexpr = $this->_get_hand_expr(true);
			$f = new func($m[1][0], $this, $subexpr);
			$val = $f->evaluate();
			$cls = gettype($val).'_value';
			return new $cls($val, $this);
		}
		return false;
	}
	
	/**
	* Scans the SQL expression for preg-escaped string $sym
	*
	* @access private
	* @return true if $sym was found ($offset is properly updated), false otherwise
	*/
	function _sym($sym) {
		if (preg_match('/\\s*('.$sym.')/A', $this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->offset = $m[1][1]+1;
			return true;
		}
		return false;
	}

	function _opening_p() {
		return $this->_sym('\\(');
	}

	function _closing_p() {
		return $this->_sym('\\)');
	}
	
	## retrieves hand expression
	function _get_hand_expr($subexpr = false) {

		$hand = $this->_get_hand();
		if ($hand === false)
			return $hand;

		if ($subexpr) {
			if ($this->_closing_p())
				return $hand;
		}

		while (preg_match('/\\s*(\\+|-|\\*|\\/|%)/A', $this->sql,
						$m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->offset = $m[1][1] + 1;
			//TODO: opening_p can happen here
			if (false === ($other_hand = $this->ev->_get_hand()))
				return $other_hand;
			$hand = new arith_expr($hand, $other_hand, $m[1][0]);
		}

		return $hand;
	}
	
	function _retrieve_value($value, &$fo, &$dest) {
/*		if (gladius_strtoupper($value)=='NULL') {
			if ($fo->not_null) {
				$this->_error(_G__FIELD_NOT_NULL, $fo->name);
				return false;
			}
			$dest = $fo->default_value;
			return true;
		}	*/

/*		if (strlen($value)) {
			if (($value[0]=="'") && ($value{strlen($value)-1}=="'"))
				$value = substr($value, 1, -1);
		}	*/

		global $GLADIUS_CUSTOM_DATA;
		if (in_array($fo->type, $GLADIUS_CUSTOM_DATA)) {
			$encfunc = 'gladius_'.$fo->type.'_enc';
			if (!$encfunc($value, $dest, $this->conn, $fo)) {
				$this->_error(_G__CONVERSION_ERROR, $fo->type);
				return false;
			}
			return true;
		}

		if ($fo->numeric) {

			switch ($fo->type) {
				case 'integer':
				case 'timestamp':
					if (!is_numeric($value)) {
						$this->_error(_G__FIELD_TYPE_MISMATCH, $fo->name, $fo->type);
						return false;
					}
					$dest = (int)$value;
					return true;
				break;
				case 'double':
					if (!is_numeric($value)) {
						$this->_error(_G__FIELD_TYPE_MISMATCH, $fo->name, $fo->type);
						return false;
					}
					if (isset($fo->max_length)) {
						$lcl = localeconv();
						$p = strpos($value, $lcl['decimal_point']);
						if ($p===false)	$p=strlen($value);
						if ($p > $fo->max_length) {
							global $GLADIUS_WARNINGS_AS_ERRORS;
							if ($GLADIUS_WARNINGS_AS_ERRORS) {
								$this->_error(_G__EXCEED_MAX_LENGTH,
										  $fo->name, $value);
								return false;
							} else
								$this->_warning(_G__EXCEED_MAX_LENGTH,
										  $fo->name, $value);
						}
					}
					$dest = (double)$value;
					return true;
				break;
				default:
					die('GRTE: invalid numeric type '.$fo->type);
			}
			$dest =  $v;
			return true;
		} else {
//			$value = $this->_unescape($value);
			switch ($fo->type) {
				case 'varchar':
				case 'text':
				case 'longtext':
					if (isset($fo->max_length)) {
						if (strlen($value) > $fo->max_length) {
							global $GLADIUS_WARNINGS_AS_ERRORS;
							$value = substr($value,0,$fo->max_length);
							if ($GLADIUS_WARNINGS_AS_ERRORS) {
								$this->_error(_G__EXCEED_MAX_LENGTH,
										  $fo->name, $value.'...');
								return false;
							} else
								$this->_warning(_G__EXCEED_MAX_LENGTH,
										  $fo->name, $value.'...');
						}
					}
					$dest = $value;
					return true;
				default:
					die('GRTE: invalid string type '.$fo->type);
			}
		}
		return false;
	}

}

	//GLADIUS NAMESPACE BEGINS
	
	function &gladius_encode(&$var) {
		// prepare the data for storage
		$s = serialize($var);
		if (strlen($s) >= _G__COMPRESSION_THRESHOLD)
			$s = gzcompress($s, 4);
		return $s;
	}

	// do not supply .php extension
	function gladius_write($fname, &$var) {
		$s =& gladius_encode($var);
		$f = @fopen($fname.'.php', 'wb');
		if ($f===false)
			return false;
		gladius_raw_write($f, $s);
		return true;
	}

	function gladius_raw_read($f,$sz) {
		$skip = strlen(_G__PHP_HEAD);
		fseek($f, $skip);
		$rd = fread($f, $sz-$skip);
		fseek($f, 0);
		if ($rd{1}==':')
			return unserialize(gzuncompress($rd));
		return unserialize($rd);
	}

	function gladius_raw_lock($fname, &$f, &$var) {
		$var = gladius_read($fname);
		$f = fopen($fname.'.php', 'rwb');
		return flock($f, LOCK_EX);
	}

	function gladius_raw_write($f, &$s) {
		fwrite($f, _G__PHP_HEAD);
		fwrite($f, $s);
		fclose($f);
	}

	function gladius_read($path) {
		$path .= '.php';
		if (!file_exists($path)) {$n=null; return $n;}
		$rd = substr(@file_get_contents($path), strlen(_G__PHP_HEAD));
		if (!is_string($rd)) {$n=null; return $n;}
		if ($rd{1}!=':')
			return unserialize(gzuncompress($rd));
		return unserialize($rd);
	}
	//GLADIUS NAMESPACE ENDS


class Gladius extends Gladius_Core {

	var $db_root;
	var $database;		// the database folder path
	var $index;
	var $_command;
	var $fetch_mode;
	var $insert_id;
	var $affected_rows;
	var $schemas;
	var $pool;			// $pool['table']['column'] = array($value1, $value2, ...);
	


	function VersionString($ver) {
		return ($ver >> 16).'.'.(($ver << 16) >> 16);
	}

	function Gladius($db = null) {
		$this->_reset();
		$this->fetch_mode = GLADIUS_FETCH_DEFAULT;
		$this->db_root = null;
		if (isset($db)) {
			//TODO: add default include paths
			$dbpath = realpath($dbpath);
			if (substr($dbpath, -1)=='/')
				$dbpath = substr($dbpat, 0, -1);
			$p = strrpos($dbpath, '/');
			$dbname = substr($dbpath, $p+1);
			$this->db_root = substr($dbpath, 0, $p);
			$this->SelectDB($dbname);
		}
	}
	
	function SetDBRoot($dbroot) {
		if (!is_dir($dbroot))
			$this->_error(_G__INVALID_DB, $dbroot);
		if (isset($this->db_root))
			$this->_reset();
		$this->db_root = realpath($dbroot).'/';
	}

	function SetFetchMode($mode) {
		$old = $this->fetch_mode;
		$this->fetch_mode = $mode;
		return $old;
	}

	function _reset() {
		$this->pool = array();
		$this->affected_rows = 0;
		$this->_command = '';
		$this->insert_id = -1;
	}

	function Describe($table) {
		if (!isset($this->database)) {
			$this->_error(_G__INVALID_DB);
			return false;
		}
		$table = gladius_strtoupper($table);
		$schema =& $this->_pick_schema($table);
		if ($schema === null) return false;
		$fields = $schema['FIELD_DEF'];
		switch ($this->fetch_mode) {
			default:
			case GLADIUS_FETCH_ASSOC:
				return $fields;
			case GLADIUS_FETCH_BOTH:
				$c=count($fields);
				for ($i=0;$i<$c;$i++) {
					$fields[$i] =& current($fields);
					next($fields);
				}
				reset($fields);
				return $fields;
			break;
			case GLADIUS_FETCH_NUM:
				return array_values($fields);
		}
	}

	function Close() {
		$this->_reset();
		$this->schemas = null;
		$this->database = null;
	}

	function _remove($fname) {
		return @unlink($this->db_root.$this->database.$fname.'.php');
	}

	function _load($fname) {
		return gladius_read($this->db_root.$this->database.$fname);
	}


	function _save($fname, &$var) {
		return gladius_write($this->db_root.$this->database.$fname, $var);
	}
	
	function _exists($fname) {
		return file_exists($this->db_root.$this->database.$fname.'.php');
	}

	function _build_index() {
		if (isset($this->index)) return;
		$this->index = $this->_load('gladius.db.master');

		if (!isset($this->index)) {
			$this->index = array('GV' => (int)((GLADIUS_HI_VER << 16) | (GLADIUS_LO_VER)),
								'TABLES' => array());
			if (!$this->_save('gladius.db.master', $this->index)) {
				$this->_error(_G__WRITE_FAILURE, 'gladius.db.master');
				return false;
			}
		}
		return true;
	}

	/* public */ function SelectDB($dbname) {
		$this->_command = 'USE';
		$old_db = $this->database;

		$database = $dbname.'/';
		
		if (!is_dir($this->db_root.$database)) {
			$this->_error(_G__CANNOT_SELECT_DB, $dbname);
			return false;
		}

		if ($old_db != $database) {
			$this->_reset();
			$this->_command = 'USE';
		}
		$this->database = $database;

//		$this->_success(count($this->index['TABLES']), 'table');
		$this->_success(1, 'database');
		return true;
	}

	/* public */ function Escape($string, $magic_quotes = false) {
		return str_replace("'", "''", $string);
	}

	/* public */ function _unescape($s) {
		return gladius_unescape($s);
	}

	function _close_ok(&$ok) {
		if (!preg_match('/\\s*(,|\\))/', $this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$this->_error(_G__MALFORMED_SQL);
			return false;
		}
		if ($m[1][0] == ')')
			$ok = true;
		$this->offset = $m[0][1]+strlen($m[0][0]);
		return true;
	}

	function _terminated() {
		if (!preg_match('/\\s*($|;)/A', $this->sql, $m, PREG_OFFSET_CAPTURE , $this->offset)) {
			$this->_error(_G__INVALID_TERMINATION);
			return false;
		}
		$this->offset += strlen($m[0][0])+$m[0][1];
		return true;
	}

	function _update_order(&$fo, &$column, &$data) {
		// reload columns in their ascending order
		$c = count($fo->ordered);
		$p_i = 0;
		$done = false;
		for ($i=0;$i<$c;$i++) {
			if ($data < $column[$fo->ordered[$i]]) {
//				array_splice($fo->ordered, $p_i, 0, $c);
				$ordered_col = array_slice($fo->ordered, 0, $p_i);
				$ordered_col[] = $c;
				$fo->ordered = array_merge($ordered_col, array_slice($fo->ordered, $p_i));
				$done = true;
				break;
			}
			$p_i = $i;
		}
		if (!$done)
			$fo->ordered[] = $c;
	}

	function _create_table($table, &$schema) {
		$empt = array();
		foreach ($schema['FIELD_DEF'] as $fo) {
			if (!$this->_save($table.'.'.$fo->name, $empt)) {
				$this->_error(_G__WRITE_FAILURE, $table.'.'.$fo->name);
				return false;
			}
			$this->pool[$table][$fo->name] = array();
		}
		$this->schemas[$table] =& $schema;
		return true;
	}

	function _parse_from(&$from) {
		while (preg_match('/\\s*(\\w+)(\\s+(as\\s+)?(\\w+))?(\\s*,)?/iA',
				$this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {

			// source table
			$src = gladius_strtoupper($m[1][0]);

			$c = count($m);
			if ($c > 2) {
				$alias = $m[4][0];
				if (gladius_reserved(gladius_strtoupper($alias))) {
					if (!empty($m[3][0])) {
						$this->_error(_G__INVALID_ALIAS, $alias);
						return false;
					}
					$from[] = array($src, '');
					$this->offset = $m[4][1];
					break;
				}
			} else
				$alias = '';

			foreach ($from as $a) {
				if ($a[0] == $src) {
					$this->_error(_G__DUPLICATE_SOURCE, $src);
					return false;
				}
			}

			$from[] = array($src, $alias);

			if ($m[$c-1][0]==',') {
				$this->offset = $m[$c-1][1]+1;
			} else {
				$this->offset = $m[$c-1][1]+strlen($m[$c-1][0]);
				break;
			}
		}

		if (empty($from)) {
			$this->_error(_G__NO_SOURCE);
			return false;
		}
		return true;
	}

	/* public */ function GetScalar($sql) {
		$r =& $this->Query($sql);
		if (is_bool($r)) {
			$this->_error(_G__INVALID_SCALAR, ($r?'TRUE':'FALSE'));
			return false;
		}
		$a = $rs->GetArray(1);
		if (count($a)==0) {
			$this->_error(_G__EMPTY_SCALAR);
			return false;
		}
		return current($a[0]);
	}

	/* public */ function &Query($sql) {
		if (!preg_match('/\\s*(\\w+)\\s*/i', $sql, $m, PREG_OFFSET_CAPTURE))
			return false;

		$this->sql =& $sql;

		$this->errno = _G__SUCCESS;
		$this->errstr = '';
		$this->offset = 0;
		$this->_command = gladius_strtoupper($m[1][0]);
		switch ($this->_command) {

			case 'SELECT':
				if (!isset($this->database)) {
					$this->_error(_G__INVALID_DB);
					$result = false;
					break;
				}
				include_once GLADIUS_DIR.'gladius_sqlops.php';
				include_once GLADIUS_DIR.'gladius_rs.php';
				$this->offset = $m[1][1]+6;
				$result =& $this->do_select(false);
				break;

			case 'UPDATE':
				if (!isset($this->database)) {
					$this->_error(_G__INVALID_DB);
					$result = false;
					break;
				}
				include_once GLADIUS_DIR.'gladius_sqlops.php';
				include_once GLADIUS_DIR.'gladius_rs.php';
				include GLADIUS_DIR.'gladius_update.php';
				break;

			case 'CREATE':
				include_once GLADIUS_DIR.'gladius_sqlops.php';
				include GLADIUS_DIR.'gladius_create.php';
				break;

			case 'DROP':
				include GLADIUS_DIR.'gladius_drop.php';
				break;

			case 'DELETE':
				if (!isset($this->database)) {
					$this->_error(_G__INVALID_DB);
					$result = false;
					break;
				}
				include_once GLADIUS_DIR.'gladius_sqlops.php';
				include_once GLADIUS_DIR.'gladius_rs.php';
				include GLADIUS_DIR.'gladius_delete.php';
				break;

			case 'INSERT':
				if (!isset($this->database)) {
					$this->_error(_G__INVALID_DB);
					$result = false;
					break;
				}
				include_once GLADIUS_DIR.'gladius_sqlops.php';
				include_once GLADIUS_DIR.'gladius_rs.php';
				include GLADIUS_DIR.'gladius_insert.php';
				break;

			case 'USE':
				$this->offset = $m[1][1]+3;
				if (!preg_match('/\\s+(\\w+)/A', $sql, $m,
								PREG_OFFSET_CAPTURE, $this->offset)) {
					$this->_error(_G__MALFORMED_SQL);
					$result = false;
					break;
				}
				$this->offset = $m[1][1]+strlen($m[1][0]);
				$result = $this->SelectDB($m[1][0]);
				break;

			case 'SHOW':
				include_once GLADIUS_DIR.'gladius_rs.php';
				include GLADIUS_DIR.'gladius_show.php';
				break;

			case 'DESCRIBE':
				include_once GLADIUS_DIR.'gladius_rs.php';
				include GLADIUS_DIR.'gladius_describe.php';
				break;

			case 'TRUNCATE':
				if (!isset($this->database)) {
					$this->_error(_G__INVALID_DB);
					return false;
				}
				include GLADIUS_DIR.'gladius_truncate.php';
				break;
				
			case 'RENAME':
				if (!isset($this->database)) {
					$this->_error(_G__INVALID_DB);
					return false;
				}
				include GLADIUS_DIR.'gladius_rename.php';
				break;
			default:
				$this->_error(_G__UNRECON_SQL, $this->_command);
				$this->_command = null;
				$result = false;
		}
		return $result;
	}

	function &do_select($subquery) {
		include GLADIUS_DIR.'gladius_select.php';
		return $result;
	}

	// load needed tables in memory and create a new resultset
	function _create_rs($from) {
		$rs = new Gladius_Resultset($this);
		
		$rs->sources = array();
		$rs->tables = array();
		// declare all FROM tables as sources
		foreach ($from as $table) {
			if (empty($table[1]))
				$alias = $table[0];
			else
				$alias = $table[1];
			$schema =& $this->_pick_schema($table[0]);
			if ($schema === null)	return false;
			$rs->sources[$alias] =& $schema;
			reset($rs->sources[$alias]['RECORD_IDS']);
			if (!in_array($table[0], $rs->tables))
				$rs->tables[] = $table[0];
			if (!empty($table[1]))
				$rs->sources[$table[0]] =& $rs->sources[$table[1]];
		}
		$rs->_reset_tables();
		return $rs;
	}

	function _lock($table) {
		$path = $this->db_root.$this->database.$table;
		if (!gladius_raw_lock($path, $f)) {
			$this->_error(_G__TABLE_LOCK_FAILED, $table);
			return null;
		}
		if (!isset($this->schemas[$table]))
			$this->schemas[$table] = gladius_raw_read($f, filesize($path.'.php'));
		return $f;
	}

	function _release_lock($f, &$var) {
		$s =& gladius_encode($var);
		gladius_raw_write($f, $s, 0x0);	// will release the lock too
	}

	function &_pick_schema($table) {
		if (!isset($this->schemas[$table])) {
			$schema = $this->_load($table);
			if (!isset($schema)) {
				$this->_error(_G__TABLE_NOT_FOUND, $table);
				$n = null;
				return $n;
			}
			
			$this->gladius_07_compat_fix = (strnatcmp($this->VersionString($schema['GV']), '0.7')<0);
			
			// apply the fix at runtime
			if ($this->gladius_07_compat_fix) {
				$fields = array_keys($schema['FIELD_DEF']);
				foreach($fields as $field) {
					$fo =& $schema['FIELD_DEF'][$field];
					if (isset($fo->default)) {
						$fo->default_value = $fo->default;
						unset($fo->default);
					}
				}
				$schema['GV'] = (int)((GLADIUS_HI_VER << 16) | (GLADIUS_LO_VER));
			}
			
			$this->schemas[$table] =& $schema;
			return $schema;
		}
		return $this->schemas[$table];
	}

	function &_pick_column($table, $column) {
		if (!isset($this->pool[$table][$column])) {
			$col = $this->_load($table.'.'.$column);
			if (!isset($col)) {
				$this->_error(_G__READ_FAILURE, $table.'.'.$column);
				$n = null;
				return $n;
			}
			$this->pool[$table][$column] =& $col;
			return $col;
		}
		return $this->pool[$table][$column];
	}

	function _success($affected, $s='row', $open = null) {
		global $GLADIUS_ERRORS;
		$this->errstr = sprintf($GLADIUS_ERRORS[_G__SUCCESS], $this->_command, $affected, $s.(($affected == 1) ? '': 's'));
		if (isset($open))
			$this->errstr.=' ('.($open ? 'open':'full').' recordset)';
		$this->affected_rows = $affected;
		$this->errno = _G__SUCCESS;
	}

/* not sure Gladius is going to have a cache subsystem

	function _cache_hash() {
		$s = substr($this->sql, 0, $this->offset);
		return sprintf('%x-%x',strlen($s),crc32($s));die;
	}
	
	function _change_rate($num_changes, $ctime) {
		return $num_changes/(time()-$ctime); // if > 0.25 then proceed to caching
	}
	
*/

}	// Gladius class declaration ends

?>
