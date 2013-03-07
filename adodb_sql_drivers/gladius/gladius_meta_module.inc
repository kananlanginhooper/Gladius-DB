<?php
/**
 * ADOdb Lite Meta Module for Gladius
 * 
 * Portions of the Meta Coding came from ADOdb
 */

/* 
  (c) 2000-2005 John Lim (jlim@natsoft.com.my). All rights reserved.
  Released under both BSD license and Lesser GPL library license. 
  Whenever there is any discrepancy between the two licenses, 
  the BSD license will take precedence. See License.txt. 
*/

eval('class gladius_meta_EXTENDER extends '. $last_module . '_ADOConnection { }');

class gladius_meta_ADOConnection extends gladius_meta_EXTENDER
{
	var $metaTablesSQL = "SHOW TABLES";	
	var $metaColumnsSQL = "DESCRIBE %s";
//var $fetchMode = false;
	function MetaError($err=false)
	{
		include_once(ADODB_DIR."/adodb-error.inc.php");
		if ($err === false)
			$err = $this->ErrorNo();

		return adodb_error($this->dataProvider,$this->databaseType,$err);
	}

	function MetaErrorMsg($errno)
	{
		include_once(ADODB_DIR."/adodb-error.inc.php");
		return adodb_errormsg($errno);
	}

	/**
	 * @returns an array with the primary key columns in it.
	 */
	function MetaPrimaryKeys($table, $owner=false)
	{
	// owner not used in base class - see oci8
		$p = array();
		$objs =& $this->MetaColumns($table);
		if ($objs) {
			foreach($objs as $v) {
				if (!empty($v->primary_key))
					$p[] = $v->name;
			}
		}
		if (sizeof($p)) return $p;
		if (function_exists('ADODB_VIEW_PRIMARYKEYS'))
			return ADODB_VIEW_PRIMARYKEYS($this->databaseType, $this->database, $table, $owner);
		return false;
	}

	/**
	 * @returns assoc array where keys are tables, and values are foreign keys
	 */
	function MetaForeignKeys($table, $owner=false, $upper=false)
	{
		return false;
	}

	function ActualType($meta)
	{
		switch(strtoupper($meta)) {
		case 'C': return 'VARCHAR';
		case 'XL': // return 'LONGTEXT';
		case 'X2': // return 'LONGTEXT';
		case 'B': // return 'LONGBLOB';
		case 'X': return 'TEXT';

		case 'C2': return 'VARCHAR';

		case 'D': return 'DATE';
		case 'DT': return 'DATETIME';
		case 'T': return 'TIME';
		case 'TS': return 'TIMESTAMP';

		case 'L': // return 'TINYINT';

		case 'R':
		case 'I4':
		case 'I': // return 'INTEGER';
		case 'I1': // return 'TINYINT';
		case 'I2': // return 'SMALLINT';
		case 'I8': // return 'BIGINT';
			return 'INT';

		case 'F': // return 'DOUBLE';
		case 'N': return 'DOUBLE';
		default: // ??
			return $meta;
		}
	}

	/**
	 * return the databases that the driver can connect to. 
	 * Some databases will return an empty array.
	 *
	 * @return an array of database names.
	 */
		function MetaDatabases() 
		{
		global $ADODB_FETCH_MODE;
		
			if ($this->metaDatabasesSQL) {
				$save = $ADODB_FETCH_MODE; 
				$ADODB_FETCH_MODE = ADODB_FETCH_NUM; 
				
				if ($this->fetchMode !== false) $savem = $this->SetFetchMode(false);
				$oldfm = $this->g->SetFetchMode(GLADIUS_FETCH_NUM);
				
				$arr = $this->GetCol($this->metaDatabasesSQL);
				if (isset($savem)) $this->SetFetchMode($savem);
				$ADODB_FETCH_MODE = $save; 
				$this->g->SetFetchMode($oldfm);
			
				return $arr;
			}
			
			return false;
		}

	/**
	 * @param ttype can either be 'VIEW' or 'TABLE' or false.
	 * 		If false, both views and tables are returned.
	 *		"VIEW" returns only views
	 *		"TABLE" returns only tables
	 * @param showSchema returns the schema/user with the table name, eg. USER.TABLE
	 * @param mask  is the input mask - only supported by oci8 and postgresql
	 *
	 * @return  array of tables for current database.
	 */
	function MetaTables($ttype=false,$showSchema=false,$mask=false)
	{
		$save = $this->metaTablesSQL;
		if ($showSchema || $mask)
			return false;

		$ret = $this->_MetaTables($ttype,false);

		$this->metaTablesSQL = $save;
		return $ret;
	}

	function _MetaTables($ttype=false,$showSchema=false,$mask=false)
	{
		global $ADODB_FETCH_MODE;

		$false = false;
		if ($mask)
			return $false;

		if ($this->metaTablesSQL) {
			
			$save = $ADODB_FETCH_MODE;
			$ADODB_FETCH_MODE = ADODB_FETCH_NUM;

			if ($this->fetchMode !== false) $savem = $this->SetFetchMode(false);
			
			$old = $this->g->SetFetchMode(GLADIUS_FETCH_NUM);

			$rs = $this->Execute($this->metaTablesSQL);
			if (isset($savem)) $this->SetFetchMode($savem);
			$ADODB_FETCH_MODE = $save;
			$this->g->SetFetchMode($old);

			if ($rs === false) return $false;
			$arr = $rs->GetArray();
			$arr2 = array();

			if ($hast = ($ttype && isset($arr[0][1]))) {
				$showt = strncmp($ttype,'T',1);
			}

			for ($i=0; $i < sizeof($arr); $i++) {
				if ($hast) {
					if ($showt == 0) {
						if (strncmp($arr[$i][1],'T',1) == 0) $arr2[] = trim($arr[$i][0]);
					} else { // VIEWS are not (yet) supported in Gladius
						if (strncmp($arr[$i][1],'V',1) == 0) $arr2[] = trim($arr[$i][0]);
					}
				} else
					$arr2[] = trim($arr[$i][0]);
			}
			$rs->Close();
			return $arr2;
		}
		return $false;
	}

	/**
	 * List columns in a database as an array of ADOFieldObjects.
	 * See top of file for definition of object.
	 *
	 * @param table	table name to query
	 * @param upper	uppercase table name (required by some databases)
	 * @schema is optional database schema to use - not supported by all databases.
	 *
	 * @return  array of ADOFieldObjects for current table.
	 */
 	function MetaColumns($table)
	{
		$this->_findschema($table,$schema);
		if ($schema) {
			$dbName = $this->database;
			$this->SelectDB($schema);
		}
		$fields = $this->g->Describe($table);

		if ($schema) {
			$this->SelectDB($dbName);
		}

		return $fields;
	}

	function _findschema(&$table,&$schema)
	{
		if (!$schema && ($at = strpos($table,'.')) !== false) {
			$schema = substr($table,0,$at);
			$table = substr($table,$at+1);
		}
	}

	/**
	  * List indexes on a table as an array.
	  * @param table  table name to query
	  * @param primary true to only show primary keys. Not actually used for most databases
	  *
	  * @return array of indexes on current table. Each element represents an index, and is itself an associative array.
	  
		 Array (
			[name_of_index] => Array
			  (
			  [unique] => true or false
			  [columns] => Array
			  (
			  	[0] => firstname
			  	[1] => lastname
			  )
		)		
	  */
     function &MetaIndexes($table, $primary = false, $owner = false)
     {
 		$false = false;
		return $false;
     }

	/**
	 * List columns names in a table as an array. 
	 * @param table	table name to query
	 *
	 * @return  array of column names for current table.
	 */ 
	function MetaColumnNames($table, $numIndexes=false,$useattnum=false /* only for postgres */) 
	{
		$objarr =& $this->MetaColumns($table);
		if (!is_array($objarr)) {
			$false = false;
			return $false;
		}
		$arr = array();
		if ($numIndexes) {
			$i = 0;
			if ($useattnum) {
				foreach($objarr as $v) 
					$arr[$v->attnum] = $v->name;
				
			} else
				foreach($objarr as $v) $arr[$i++] = $v->name;
		} else
			foreach($objarr as $v) $arr[strtoupper($v->name)] = $v->name;
		
		return $arr;
	}

	function MetaTransaction($mode,$db)
	{
		return false;
	}


}

eval('class gladius_meta_resultset_EXTENDER extends '. $last_module . '_ResultSet { }');

class gladius_meta_ResultSet extends gladius_meta_resultset_EXTENDER
{
	/**
	 * Get the metatype of the column. This is used for formatting. This is because
	 * many databases use different names for the same type, so we transform the original
	 * type to our standardised version which uses 1 character codes:
	 *
	 * @param t  is the type passed in. Normally is ADOFieldObject->type.
	 * @param len is the maximum length of that field. This is because we treat character
	 * 	fields bigger than a certain size as a 'B' (blob).
	 * @param fieldobj is the field object returned by the database driver. Can hold
	 *	additional info (eg. primary_key for mysql).
	 * 
	 * @return the general type of the data: 
	 *	C for character < 250 chars
	 *	X for teXt (>= 250 chars)
	 *	B for Binary
	 * 	N for numeric or floating point
	 *	D for date
	 *	T for timestamp
	 * 	L for logical/Boolean
	 *	I for integer
	 *	R for autoincrement counter/integer
	 * 
	 *
	*/
	function MetaType($t,$len=-1,$fieldobj=false)
	{
		if (is_object($t)) {
			$fieldobj = $t;
			$t = $fieldobj->type;
			$len = $fieldobj->max_length;
		}
		$is_serial = is_object($fieldobj) && $fieldobj->primary_key && $fieldobj->auto_increment;

		$len = -1; //  max_length is not accurate
		switch (strtolower($t)) {
		case 'varchar':
			return 'C';
		case 'text':
			return 'X';

		// 'B' binary not yet supported in Gladius

		case 'date': return 'D';

		case 'time': return 'T';
		case 'datetime':
			return 'DT';
		case 'timestamp': return 'TS';

		case 'double':
			return 'F';

		case 'int':
			return $is_serial ? 'R' : 'I';
//		default:	return 'N';
		}
	}

}

?>