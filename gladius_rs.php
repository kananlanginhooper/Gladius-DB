<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# Flatfile php database engine SQL92 compliant
#
# Resultset class
#

class Gladius_Resultset extends Gladius_Core
{
	var $fetch_mode;
	var $fetch_func;
	var $EOF;
	var $BOF;

	var $where_fields;
	var $order_fields;
	var $data_fields;
	var $_calc_fields;
	var $limit_from;
	var $limit_length;
	var $rowset;
	var $current_row;
	var $row_count;
	var $row_index;
	var $keys;

	var $t;				// current head table queried
	var $tot_t;			// count of tables
	var $EOR;
	var $iteration_arg;
	var $conn;
	var $_aggregate;	// true if aggregate functions are used
	var $in_subquery;

	var $where;			// filtering tree
	var $allfields;
	var $fields;
	var $tables;		// table/aliases	names
	var $sources;


	/**
	 * gladiusResultSet Constructor
	 *
	 * @access private
	 * @param string $record
	 * @param string $resultId
	 */

	function Gladius_ResultSet(&$conn)
	{
		$this->_link($conn);
		$this->_class_init();
//		$this->conn =& $conn;

		$this->ev = new Gladius_Eval($conn, $this->sources, $this->where_fields, $this->allfields);

		$this->conn =& $conn;
		$this->fetch_mode = $conn->fetch_mode;
		global $GLADIUS_FETCH_FUNC;
		$this->fetch_func = $GLADIUS_FETCH_FUNC[$this->fetch_mode];
	}

	function _class_init() {
		$this->fields = array();		// field definitions involved
		$this->allfields = array();		// both result and where fields here
		$this->data_fields = array();	// straight data fields
		$this->rowset = array();

		$this->where = new begin_expr($this);
		$this->where_fields = array();
		$this->_calc_fields = array();
		$this->_aggregate = false;
		$this->order_fields = array();
		$this->iteration_arg = array();
		$this->order_way = array();
		$this->in_subquery = false;

		$this->keys = array();

		$this->current_row = 0;
		$this->EOF = false;
		$this->BOF = true;
		$this->EOR = false;
	}

	/**
	 * Frees resultset
	 *
	 * @access public
	 */

	function Close()
	{
		$this->_class_init();
	}

	/**
	 * Returns field name from select query
	 *
	 * @access public
	 * @param int $field (column)
	 * @return string Field name
	 */

	function Fields( $field = null )
	{
		if (!isset($field))
			return $this->aliases;
		else
			return $this->fields[$field]->name;
	}

	/**
	 * Returns numrows from select query
	 *
	 * @access public
	 * @return integer Numrows
	 */
	 
	function RecordCount() {
		if (!$this->EOR) {
//			while ($this->_iterate('_fetch_row')) {	}
//			$this->affected_rows = $this->row_count;
			$this->_precache_rowset();
		}
		$this->affected_rows = $this->row_count;
		return $this->row_count;
	}
	
	/**
	 * Returns num of fields from select query
	 *
	 * @access public
	 * @return integer numfields
	 */

	function FieldCount()
	{
		return count($this->fields);
	}

	/**
	 * Returns next record
	 *
	 * @access public
	 */

	function MoveNext()
	{
		return $this->Move($this->current_row + 1);
	}

	/**
	 * Move to the first row in the recordset. Many databases do NOT support this.
	 *
	 * @return true or false
	 */

	function MoveFirst()
	{
		return $this->Move(0);
	}

	/**
	 * Returns the Last Record
	 *
	 * @access public
	 */

	function MoveLast()
	{
		return $this->Move($this->RecordCount() - 1);
	}

	/**
	 * Random access to a specific row in the recordset.
	 *
	 * @param rowNumber is the row to move to (0-based)
	 *
	 * @return true if there still rows available, or false if there are no more rows (EOF).
	 */
	
	function Move($rowNumber) {
		if ($rowNumber == $this->current_row)
			return true;

		// if a row > than total read rows is queried
		if ($rowNumber >= $this->row_count) {

			// if the recordset was finished then set EOF
			if ($this->EOR) {
				$this->current_row = $this->row_count-1;
				$this->EOF = true;
				return false;
			}

			// affected_rows = previous row_count (see after while)
			$this->affected_rows = $this->row_count;
			while ($this->_iterate('_fetch_row')) {
				// fetch rows until the selected row number is matched
				if ($this->row_count-1==$rowNumber) {
					$this->current_row = $rowNumber;
					// reset EOF flag
					$this->EOF = false;
					return true;
				}
			}
			// affected_rows = number of newly read rows
			$this->affected_rows = $this->row_count - $this->affected_rows;
			// current_row = last read row
			$this->current_row = $this->row_count - 1;
			$this->EOF = true;
			return false;
		}
		// invalid row number (negative)
		if ($rowNumber<0)
			return false;

		// an already existing row is being queried
		$this->EOF = false;
		$this->current_row = $rowNumber;

		return true;
	}

	function _reset_tables() {
//		rsort($this->tables);	// ?
		$this->tot_t = count($this->tables);
		$this->t = 0;
		foreach ($this->tables as $table)
			reset($this->sources[$table]['RECORD_IDS']);
	}

	function _update_row() {
		$this->_delete_row();
		foreach($this->ops as $key => $hand) {
			$fo =& $this->allfields[$key];
//			$id = $this->sources[$this->tables[$this->t]]['RECORD_IDS'][$this->row_index];
			if (!$this->ev->_retrieve_value($hand->evaluate(), $fo, $val))
				return false;

			$this->updated[$key][] = $val;
		}
		return true;
	}

	function _delete_row() {
		if (!empty($this->iteration_arg)) {
			$last =& $this->iteration_arg[count($this->iteration_arg)-1];
			if ($this->row_index == $last[0]+$last[1]) {
				$last[1]++;
				return true;
			}
		}
		$this->iteration_arg[] = array($this->row_index, 1);
		return true;
	}

	function recompile_rowset() {
		$compiled = array();
		if ($this->limit_length)
			$max = min($this->row_count, $this->limit_from + $this->limit_length);
		else
			$max = $this->row_count;
		for($i=$this->limit_from;$i<$max;$i++) {
			$record = array();
			switch ($this->fetch_mode) {
				case GLADIUS_FETCH_BOTH:
					$record = array_slice($this->rowset[$i], 0, count($this->aliases));
					$c = 0;
					foreach ($this->aliases as $field) {
						$record[$field] =& $record[$c++];
					}
				break;
				case GLADIUS_FETCH_NUM:
					$record = array_slice($this->rowset[$i], 0, count($this->aliases));
				break;
				default:
				case GLADIUS_FETCH_DEFAULT:
				case GLADIUS_FETCH_ASSOC:
					$c = 0;
					foreach ($this->aliases as $field) {
						$record[$field] = $this->rowset[$i][$c++];
					}
				break;
			}
			$compiled[] = $record;
		}
		unset($this->rowset);
		$this->rowset =& $compiled;
		$this->row_count = $max;
	}

	// called when $this->keys is not empty
	function _decode_record(&$record) {
		foreach ($this->keys as $key => $field) {
			$fo =& $this->fields[$key];
			$decfunc = 'gladius_'.$fo->type.'_dec';
			if (!$decfunc($record[$field], $record[$field], $this->conn, $fo))
				die('GRTE: fatal error while decoding');
		}
	}

	// rowset is built using numerical arrays, then it is eventually re-compiled using
	// the requested array keys
	function _order_fetch() {
	
		$this->_fetch_fields($this->iteration_arg);
		$this->_prefetch_calculated_fields();
		
		$record = array();
		foreach($this->fields as $fo) {
			$record[] = $fo->value;
		}
		foreach($this->order_fields as $field) {
			$record[] = $this->allfields[$field]->value;
		}
/*		if ($this->fetch_mode != GLADIUS_FETCH_NUM) {
			reset($fields);
			$i=0;
			foreach ($fields as $field)
				$record[$field] =& $record[$i++];
		}*/
		$this->rowset[] = $record;
		return true;
	}

	function &_fetch_both() {
		$i=0;
		foreach ($this->fields as $fo) {
			$record[$i] = $fo->value;
			$record[$fo->alias] =& $record[$i];
			$i++;
		}
		$vf =& $record;
		return $vf;
	}

	function &_fetch_num() {
		foreach ($this->fields as $fo) {
			$record[] = $fo->value;
		}
		$vf =& $record;
		return $record;
	}

	function &_fetch_assoc() {
		foreach ($this->fields as $fo) {
			$record[$fo->alias] = $fo->value;
		}
		$vf =& $record;
		return $record;
	}
	
	function _calculate_fields() {
		$c=count($this->_calc_fields);
		if(!$c)	return;
		// in case of counting only just precache the row indexes
		if ($this->_row_counting()) {
                        $this->rowset = array( array(count($this->_precached_row_indices)));
			$this->row_count = 1;
			return;
		}
		
		// before modifying the rowset, compile the function callbacks
		$functions = array();
		for($i=0;$i<$c;++$i) {
			$cf =& $this->_calc_fields[$i];
			$fn = $cf->arg_field->name;
			unset($cf->arg_field);
			$cf->arg_field = $fn;
			$functions[$cf->field_index] =& $cf;
			$cf->values = array();
		}
		
		// start parsing values from the rowset
		$c=$this->row_count;
		$fc=count($this->fields);
		reset($this->rowset);
		do {
			// select (by reference) one row
			$row =& $this->rowset[key($this->rowset)];
			
			$ri=0;
			reset($row);
			while ($ri<$fc) {
				// if there is a function set for that field index
				if (isset($functions[$ri])) {
					// add the value for that column
					$v =& $row[key($row)];
					$functions[$ri]->values[] =& $v;
//					$row[key($row)] =& $v;
				}
				++$ri;
				next($row);
			}
		} while (next($this->rowset) !== false);
		// now execute the aggregate functions
		reset($functions);
		do {
			$cf =& $functions[key($functions)];
			if (!call_user_func('gladius_sql_'.$cf->func, $cf->values)) {
				// ERROR here!
				die("GRTE: fatal error in aggregate function ".$cf->func);
			}
		} while (next($functions)!==false);
		
	}
	
	function _prefetch_calculated_fields() {
		$c=count($this->_calc_fields);
		for($i=0;$i<$c;++$i) {
			$fo =& $this->_calc_fields[$i];
			$fo->value = $fo->arg_field->value;
		}
	}

	// MAIN FETCH FUNCTION
	// load values from fields and compile a new row of the rowset
	function _fetch_row() {
		// get the field descriptors
		$this->_fetch_fields($this->iteration_arg);
		$this->_prefetch_calculated_fields();

		// call the method to fetch that particular field
		$record = $this->{$this->fetch_func}();

		// add the record to rowset
		$this->rowset[] = $record;
		return true;
	}

	// should be optimized
	function _fetch_fields($fields) {
		// copy data into the fields
		foreach($fields as $field) {
			$column =& $this->conn->pool[$this->allfields[$field]->table][$field];
			$this->allfields[$field]->value = $column[$this->row_index];
		}
	}

	// remove some unused members
	function _compact() {
		// commented because would affect post-fetching
		//unset($this->iteration_arg);
		unset($this->where_fields);
		unset($this->allfields);
		unset($this->tables);
		unset($this->sources);
		unset($this->ev);
	}

	function initiate() {
		// relative cursor in resultset being built
		$this->row_count = 0;
		$this->_reset_tables();
//		$this->row_index = key($this->sources[$this->tables[$this->t]]['RECORD_IDS']);
		$this->EOR = (current($this->sources[$this->tables[$this->t]]['RECORD_IDS']) === false);
	}

	function _upd_cursor() {
		$o =& $this->sources[$this->tables[$this->t]];
/*		if (!isset($o)) {
			$o =& $this->sources[$this->tables[0]];
//			var_dump($o);die;
			var_dump($this->tot_t);die;
		}	*/
		$this->row_index = key($this->sources[$this->tables[$this->t]]['RECORD_IDS']);
		$this->EOR = (next($this->sources[$this->tables[$this->t]]['RECORD_IDS']) === false);
	}

	// MAIN ITERATION FUNCTION
	// fetch the next row
	function _iterate( $iterate_method ) {
		$skipped = 0;
		$delta = 1;

		do {
			if (($this->limit_length - $this->row_count == 0) && ($this->row_count!=0)) {
				$this->EOR = true;
//				$this->_compact();
				return false;
			}

			// if one table has gone EOF get the next one
			if ($this->EOR) {
				for ($it = $this->t; $it < $this->tot_t; $it++) {
					reset($this->sources[$this->tables[$it]]['RECORD_IDS']);
				}
				$this->t++;
				// if all tables have gone EOF, end querying of the resultset
				if ($this->t == $this->tot_t)
					return false;
				$this->_upd_cursor();
				$this->EOR = false;
				continue;
			}

			$this->_upd_cursor();

			// load the where fields
			$this->_fetch_fields($this->where_fields);
			// test the next row
			if (!$this->where->test())
				continue;
			// check if we need to skip the row
			if ($skipped < $this->limit_from) {
				$skipped++;
				continue;
			}

			// iterate with the selected method
			if (!$this->$iterate_method())
				return false;
			// increment row count
			$this->row_count++;
			break;
		} while (true);
		return true;
	}

	/**
	 * Check to see if last record reached
	 *
	 * @access public
	 */

	function EOF()
	{
		if( $this->current_row < $this->row_count)
			return false;
		else {
			$this->EOF = true;
			return true;
		}
	}

	/**
	 * Returns All Records in an array
	 *
	 * @access public
	 * @param [nRows]  is the number of rows to return. -1 means every row.

	 */

	function GetArray($nRows = -1)
	{
		if ($this->EOF)
			return array();
		$bak = $this->current_row;
		if ($nRows == -1) {
			$nRows = $this->RecordCount() - $bak;
			$this->current_row = $this->row_count - 1;
			$this->EOF = true;
			if ($this->row_count == 0)
				return array();
		} else if ($nRows < $this->row_count - $bak) {
			if (!$this->Move($bak + $nRows))
				$nRows = $this->current_row - $bak;
		} else {
			$this->Move($bak + $nRows);
		}
/*		if ($nRows>1) {
			$dbg = @debug_backtrace();
			if ($dbg) {
				$dbginfo = @$dbg[1]['file'].':'.@$dbg[1]['line'].' '.$dbg[1]['function'].'() ';
			} else
				$dbginfo = '';
			// replace with trigger_error
			$this->_log($dbginfo.'It is advised to use MoveNext() when iterating through resultset rows', E_USER_NOTICE);
		}	*/
		
		// get the real rows if precaching was active
		$this->_fetch_precached();

		if (empty($this->keys))
			return array_slice($this->rowset, $bak, $nRows);
		$r = array_slice($this->rowset, $bak, $nRows);
		$c=count($r);
		for($i=0;$i<$c;$i++) {
			$this->_decode_record($r[$i]);
		}
		return $r;
	}

	function GetRow() {
		$row_a = $this->GetArray(1);
		if (empty($row_a))
			return array();
		return $row_a[0];
	}

	function GetRows($nRows = -1)
	{
		return $this->GetArray($nRows);
	}

	function GetAll()
	{
		return $this->GetArray();
	}

	/**
	* Fetch field information for a table.
	*
	* @return object containing the name, type and max_length
	*/
	function FetchField($fieldOffset = -1)
	{
		if ($field_offset == -1)
			return array_values($this->fields);

		while (($field_offset>0) && next($this->fields)) {
			$field_offset++;
		}
		$fieldObject = current($this->fields);
		reset($this->fields);
		return $fieldObject;
	}

	function _parse_where_expr() {

		if (!preg_match('/\\s*WHERE\\s+(.)/iA',
					$this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset))
			return true;
		$this->offset = $m[1][1];
		
		if (!$this->in_subquery)
			$last_exp = ';?\\s*$';
		else
			$last_exp = '\\s*\\)';
		
		// initialize the head of the chain and the nesting stack
		$current =& $this->where;
		$boolean_op = 'and';
		$stack = array();
		
//		$d=($this->_command=='SELECT');
		
//		echo '<b>PARSED CHAIN <big>[</big></b><br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;';

		// parse the expressions (also with nested parentheses)
		do {
			while ($this->ev->_opening_p()) {
//				echo '<big style="color:green; font-weight:bold">CHAIN(</big>';
				$current->bool_op = $boolean_op;
				// DEBUG code
/*				if (isset($current->next)) {
					echo '<pre>OVRW OPEN:<br>';var_dump($current->next);echo '</pre>';die;
				} */
				// append the subexpression container to the current expression
				$current->next = new begin_expr($this);
				$subexpression =& $current->next;
				// save the parent expression reference
				$stack[] =& $current;
				// set the current reference to begin_expr::subexpr
				$subexpression->subexpr = new begin_expr($this);
				$current =& $subexpression->subexpr;
				$boolean_op = null;
			}
			
			$current->bool_op = $boolean_op;
			$current->next = $this->_parse_one_expr();
//			echo '<pre>EXP:<br>';var_dump($current->next);echo '</pre>';
			if (!isset($current->next)) {
//				die('GRTE: unparsed expression');
				// error already set by ::parse_one_expr()
				return false;
			}
			$current =& $current->next;
			
			while ($this->ev->_closing_p()) {
//				echo '<big style="color:green; font-weight:bold">)</big>';
				if (empty($stack)) {
					if ($this->in_subquery) {
						$this->offset--;
						return true;
					}
					$this->_error(_G__UNEXPECTED_CLOSEP);
					return false;
				}

				// get the parent (e.g. the last expression in the linear chain)
				$parent = array_pop($stack);
				// get reference to subexpression (begin_expr)
				$subexpression =& $parent->next;
				// set the current (empty) expression to the begin_expr's next slot
				$current =& $subexpression;
/*				echo '<pre>';var_dump($current->next);echo '</pre>';die;
				if (isset($current->next)) {
					debug_chain($this->where->next);
					echo '<br>';
					debug_chain($current->next);
					echo '<br>';echo '<br>';echo '<br>';
					//echo '<pre>OVRW CLOSE:<br>';var_dump($current->next);echo '</pre>';
					die;
				}	*/
				
			}

			// subquery parenthesis are needed
			if (preg_match('/\\s*(ORDER|GROUP|LIMIT|'.$last_exp.')/A', gladius_strtoupper(substr($this->sql, $this->offset, 200)),
					$ignored, 0)) {
				break;
			}

			if (false === ($boolean_op = $this->_get_boolean($this->sql))) {
				$this->_error(_G__BOOL_OP_EXPECTED);
				return false;
			}

		} while (true);
		
//		echo '<br/><b><big>]</big></b><br/>';
		return true;
	}

	function _get_boolean(&$expr) {
		$boolean = $this->_get_boolean_op($expr);
		if (!$boolean)
			return false;
		$backup = $this->offset;
		$not = $this->_get_boolean_op($expr);
		if ($not == 'not')
			$boolean = 'n'.$boolean;
		else
			$this->offset = $backup;

		global $GLADIUS_BOOL_OPS;
		if (!in_array($boolean, $GLADIUS_BOOL_OPS)) {
			$this->_error(_G__INVALID_BOOL_OP, $boolean);
			return false;
		}

//		echo '<i style="font-family:tahoma">'.gladius_strtoupper($boolean).'</i> ';
		return $boolean;
	}

	## parse one expression
	function _parse_one_expr() {

		$left = $this->ev->_get_hand_expr();
		if ($left === false) {
//			$this->_error(_G__LHAND_EXPECTED);
			return null;
		}
		
		if (!preg_match('/\\s*(=|<>|<=|>=|>|<)/A', $this->sql,
						$m, PREG_OFFSET_CAPTURE, $this->offset)) {
			if (!preg_match('/\\s*(not\\s+)?(like|in|between)/iA', $this->sql,
				$m, PREG_OFFSET_CAPTURE, $this->offset)) {
					$this->_error(_G__OPERATOR_EXPECTED);
					return null;
			} else {
				$raw_operator = gladius_strtolower($m[2][0]);
				$this->offset = $m[2][1]+strlen($m[2][0]);
				$neg = (!empty($m[1][0]));

				switch ($raw_operator) {
					case 'in':
						if (!preg_match('/\\s*\\(\\s*/A', $this->sql, $m, 0, $this->offset)) {
							$this->_error(_G__EXPECTED_OPP);
							return null;
						}
						$this->offset += strlen($m[0]);
						$values = false;
						if (preg_match('/select\\s+(.)/iA', $this->sql,
							$m, PREG_OFFSET_CAPTURE, $this->offset)) {
							include_once GLADIUS_DIR.'gladius_sqlops.php';
							// move the offset to the first field in the SELECT statement
							$this->offset = $m[1][1];
							$old_fm = $this->conn->SetFetchMode( GLADIUS_FETCH_NUM );
								$rs = $this->conn->do_select(true);
							$this->conn->fetch_mode = $old_fm;
							if ($rs === false)
								return null;
							if (count($rs->fields)> 1) {
								$this->_error(_G__TOO_MANY_FIELDS_SUBQ);
								return null;
							}

							if ($rvalues = $rs->GetArray()) {
								foreach($rvalues as $row)
									$values[] =& $row[0];
								unset($rvalues);
							} else $values = array();
							unset($rs);
						} else { // parse a simple values list
							while (false !== ($item = $this->ev->_get_immediate())) {
								$values[] = $item->evaluate();
								if (!preg_match('/\\s*,\\s*/A', $this->sql, $m, 0, $this->offset))
									break;
								$this->offset += strlen($m[0]);
							}
							if (!preg_match('/\\s*\\)\\s*/A', $this->sql, $m, 0, $this->offset)) {
								$this->_error(_G__MISS_CLS);
								return null;
							}
							$this->offset += strlen($m[0]);
						}
						if ($values === false) {
							$this->_error(_G__SET_EXPECTED);
							return null;
						}

						if ($neg)
							$raw_operator = 'nin_op';
						else
							$raw_operator = 'in_op';
						return new $raw_operator($left, $values);
					break;
					case 'like':
						$this->offset = $m[0][1]+strlen($m[0][0]);

						$right = $this->ev->_get_hand_expr();
						if ($right === false) {
							$this->_error(_G__RHAND_EXPECTED);
							return null;
						}
						
						if ($neg)
							$raw_operator = 'nlike';
						else
							$raw_operator = 'like';
					break;
					case 'between':
						$l = $this->ev->_get_hand_expr();
						if ($l === false) {
							$this->_error(_G__LHAND_EXPECTED);
							return null;
						}
						if (!preg_match('/\\s+and\\s+/iA', $this->sql,
							$m, 0, $this->offset)) {
							$this->_error(_G__AND_EXPECTED);
							return null;
						}
						$this->offset += strlen($m[0]);
						$r = $this->ev->_get_hand_expr();
						if ($r === false) {
							$this->_error(_G__RHAND_EXPECTED);
							return null;
						}
						if ($neg) {
							$a = '<=';
							$b = '>=';
						} else {
							$a = '>=';
							$b = '<=';
						} // convert the BETWEEN to a double comparison
						$bw = new expr ($left, $l, $a);
						$bw->bool_op = 'and';
						$bw->next = new expr ($left, $r, $b);
						return $bw;
				}
			}
		} else {
			$raw_operator = $m[1][0];
			
			$this->offset = $m[1][1]+strlen($m[1][0]);

			$right = $this->ev->_get_hand_expr();
			if ($right === false) {
				$this->_error(_G__RHAND_EXPECTED);
				return null;
			}
		}


//		echo '<b>EXPR</b> (('.get_class($left).')'.$left->content;
//		echo ' '.$raw_operator.' ('.get_class($right).')'.$right->content.')';

		return new expr($left, $right, $raw_operator);
	}

	function _get_boolean_op(&$expr) {

		if (preg_match('/\\s*(\\w+)\\s*/iA', $expr, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
			$boolean = gladius_strtolower($m[1][0]);
			$this->offset = $m[1][1]+strlen($m[1][0]);
			return $boolean;
		}
		return false;
	}
	
	function _parse_group_expr() {
		return true;
	}

	function _parse_order_expr() {
		if (!preg_match('/\\s*order\\s+by\\s+/iA',
					$this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset))
			return true;
		$this->offset = $m[0][1]+strlen($m[0][0]);

		$fields = array_flip(array_keys($this->fields));
		
		while (preg_match('/\\s*(\\w+)\\s*(ASC|DESC)?\\s*(,)?/iA',
			$this->sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {

			$field = $m[1][0];
			for($t=0;$t<$this->tot_t;++$t) {
				$schema =& $this->sources[$this->tables[$t]];
				if (!isset($schema['FIELD_DEF'][$field])) {
					$this->_error(_G__FIELD_NOT_FOUND, $field);
					return false;
				} else {
					$fo =& $this->sources[$this->tables[$t]]['FIELD_DEF'][$field];
					break;
				}
			}

			if (!isset($fields[$fo->name])) {
				$this->order_fields[] = $fo->name;
				$this->order_ids[] = count($fields) + count($this->order_fields) - 1;
			} else
				$this->order_ids[] = $fields[$fo->name];
			$this->allfields[$fo->name] =& $fo;

			$c = count($m);
			$this->offset = $m[$c-1][1]+strlen($m[$c-1][0]);
			if ($c>2) {
				if (gladius_strtolower($m[2][0])=='desc')
					$this->order_way[] = SORT_DESC;
				else // ASC already checked by the regular expression
					$this->order_way[] = SORT_ASC;
				if ($c<4)
					break;
				if ($m[3][0] != ',') {
					$this->_error(_G__MALFORMED_SQL);
					return false;
				}

			} else {
				$this->order_way[] = SORT_ASC;
				break;
			}
		}

		return true;
	}

	function _parse_limit() {
		if (preg_match('/\\s*LIMIT\\s+(\\d+)\\s*,?\\s*([\\+\\-]?\\d+)?/A',
			gladius_strtoupper($this->sql), $m, PREG_OFFSET_CAPTURE, $this->offset)) {

			$c=count($m);
			if ($c==3) {
				$limit_from = (int)$m[1][0];
				if ( 0 >= ($limit_length = (int)$m[2][0]) )
					return false;
			} else {
				$limit_from = 0;
				if (0 >= ($limit_length = (int)$m[1][0]))
					return false;
			}
			$this->offset = $m[$c-1][1]+strlen($m[$c-1][0]);
		} else {
			$limit_from = 0;
			$limit_length = 0;
		}
		return array($limit_from, $limit_length);
	}
	
	function _find_field($field, &$table) {
		$table = null;
		for($t=0;$t<$this->tot_t;++$t) {
			$table_name = $this->tables[$t];
			$schema =& $this->sources[$table_name];
			if (isset($schema['FIELD_DEF'][$field])) {
				if (isset($table)) {
					$this->_error(_G__AMBIGUOUS_FIELD, $field);
					return false;
				}
			$table = $table_name;
			}
		}
		if (!isset($table)) {
			$this->_error(_G__FIELD_NOT_FOUND, $field);
			return false;
		}
		return true;
	}


	function _add_all_fields($table) {
		foreach (array_keys($this->sources[$table]['FIELD_DEF']) as $field) {
			$fo =& $this->sources[$table]['FIELD_DEF'][$field];
			$this->fields[$fo->name] =& $fo;
			$this->allfields[$fo->name] =& $fo;
			$this->aliases[] = $fo->alias;
		}
	}
	
	function _load_field_expr($expr, &$found_asterisk, &$table, &$field, $counting=false) {
			$a = explode('.', $expr, 3);
			switch (count($a)) {
				case 1:
					if ($a[0]=='*') {
						if ($found_asterisk) {
							$this->_error(_G__ASTERISK_AGAIN);
							return 0;
						}
						$found_asterisk = true;
						if ($counting) {
							//TODO: apply some optimization to COUNT(*)
							$table = $this->tables[0];
							$field = current(array_keys($this->sources[$table]['FIELD_DEF']));
//							die('GRTE: asterisk in aggregate functions not yet supported');
							return 1;
						}
						foreach($this->tables as $table_name)
							$this->_add_all_fields($table_name);
						return 2;
					}
					$field = $a[0];
					if (!$this->_find_field($field, $table))
						return 0;
				break;
				case 2:
					$table = gladius_strtoupper($a[0]);
					if (!isset($this->sources[$table])) {
						$this->_error(_G__TABLE_NOT_SELECTED, $table);
						return 0;
					}
					$fdefs =& $this->sources[$table]['FIELD_DEF'];
					if ($a[1]=='*') {
						if ($counting) {
							$field = current(array_keys($fdefs));
							return 1;
						}
						$this->_add_all_fields($table);
						return 2;
					}
					$field = $a[1];
				
					if (!isset($fdefs[$field])) {
						$this->_error(_G__FIELD_NOT_FOUND, $field);
						return 0;
					}
				break;
				default:
					$this->_error(_G__MALFORMED_SQL);
					return 0;
			}
			
			return 1;
	}
	
	function _parse_function($func, &$obj, $fi) {
		preg_match('/(\\w+)\\s*\\(\\s*('._G__FIELD.')\\s*\\)\\s*$/A', $func, $m);
		$fname = gladius_strtolower($m[1]);
		if (!function_exists('gladius_sql_'.$fname)) {
			die("GRTE: invalid aggregate function ".$fname);
		}
	
		$obj = new stdclass();
		$this->_calc_fields[] =& $obj;
		$obj->alias = $obj->name = $func;
		$obj->value = $obj->type = null;
		$obj->field_index = $fi;
		$obj->func = $fname;
		$obj->arg_field = $m[2];	
		return true;
	}

	function _load_fields(&$fields,&$aliases) {
		$found_asterisk = false;
		$aliases = array();
		$fi = 0;
		foreach ($fields as $fld) {
			$alias = $fld[1];

			// parse aggregate functions
			if ($fld[2]) {
				if (!$this->_parse_function($fld[0], $obj_f, $fi))
					return false;
				
				$to_load = $obj_f->arg_field;
			} else
				$to_load = $fld[0];

			++$fi;
			
			$rv = $this->_load_field_expr($to_load, $found_asterisk, $table, $field, $fld[2]);
			if (!$rv) return false;
			else {
				// adding all fields of this table
				if ($rv==2) continue;
			}
			// save this straight field name
			if ($to_load != '*')
				$this->data_fields[] = $to_load;

			// if it is continuing the field must be referenced and saved
			$fo =& $this->sources[$table]['FIELD_DEF'][$field];
			$this->allfields[$fo->name] =& $fo;
			if (!$fld[2])
				$obj_f =& $fo;
			else
				$obj_f->arg_field =& $fo;
			
//			$fo->ref = true;
			if (isset($alias[0])) {
				// maybe this check is too much restrictive??
				if (in_array(gladius_strtolower($alias), $aliases)) {
					$this->_error(_G__AMBIGUOUS_ALIAS, $alias);
					return false;
				}
				$obj_f->alias = $alias;
			}
			$aliases[] = $obj_f->alias;
			
			$this->fields[$obj_f->name] =& $obj_f;
		}
		return true;
	}

	function _remove_order(&$fo, &$ranges) {
		$new_order = array();
		foreach ($fo->ordered as $i) {
			$delta = 0;
			foreach($ranges as $range) {
				if (($i >= $range[0]) && ($i<$range[0]+$range[1]))
					continue 2;
				if ($i < $range[0])
					break;
				$delta += $range[1];
			}
			$new_order[] = $i - $delta;
		}
		$fo->ordered = $new_order;
	}
	
	var $_precached_row_indices = array();
	
	// apply only the 'WHERE' expression and get the row index
	function _precache_rowset() {
		//FIXME: why necessary?
		// reset tables counter
		$this->t = 0;
		while ($this->_iterate('_prefetch_row')) { }
	}

	var $_is_counting;
	function _row_counting() {
		if (!isset($this->_is_counting)) {
			$this->_is_counting = false;
			if (count($this->_calc_fields)==1) {
				$af =& $this->_calc_fields[0];
				if ($af->func == 'count') {
					$this->_precache_rowset();
					$this->_is_counting = true;
				}
			}
		}
		return $this->_is_counting;
	}
	
	function _prefetch_row() {
		// save the current row index
		$this->_precached_row_indices[] = $this->row_index;
		return true;
	}
	
	## this is like a normal row fetch, but WHERE has already been evaluated and we
	## have saved the valid indices in _precached_row_indices
	function _fetch_precached() {
		// also fields which were in WHERE expression need to be updated
		$this->iteration_arg = array_unique(array_merge($this->iteration_arg, array_merge($this->where_fields, $this->data_fields)));
		foreach($this->_precached_row_indices as $i) {
			// hardly set row index
			$this->row_index = $i;
			// get the field descriptors
			$this->_fetch_fields($this->iteration_arg);
			$this->_prefetch_calculated_fields();

			// call the method to fetch that particular field
			$record = $this->{$this->fetch_func}();

			// add the record to rowset
			$this->rowset[] = $record;
		}
		unset($this->_iteration_arg);
	}

}
?>
