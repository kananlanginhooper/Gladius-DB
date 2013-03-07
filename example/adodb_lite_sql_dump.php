<?php
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# Example of adoDB lite usage and SQL dumping through it
# This script will dump the default gtestdb to stdout
#

// set $idir to the directory of adoDB lite
define('_VALID', 1);
$idir = '../../../../laniuscms/classes/';

// include adoDB
require $idir.'adodb_lite/adodb-errorhandler.inc.php';
require $idir.'adodb_lite/adodb.inc.php';

require '../gladius.php'; 	 
require '../gladius_rs.php'; 	 

$ADODB_FETCH_MODE = ADODB_FETCH_ASSOC;

$db =& ADONewConnection('gladius', 'pear:meta');
$db->PConnect('','','', 'gtestdb');

// workaround for adoDB lite's bug 1707315 
// bug tracker item: http://sourceforge.net/tracker/index.php?func=detail&aid=1707315&group_id=140982&atid=747945
$db->adodb->fetchMode = $ADODB_FETCH_MODE;

// following code from Lanius CMS DbBackup class

function dkdb_encode($s) {
	return str_replace("'", "''", $s);
}

function _meta_numeric_fix(&$meta) {
	if (!isset($meta->numeric)) {
		$meta->numeric = (	(strpos($meta->type, 'int')===0) ||
							(strpos($meta->type, 'tinyint')===0) ||
							(strpos($meta->type, 'float')!==false) ||
							(strpos($meta->type, 'numeric')!==false) ||
							(strpos($meta->type, 'decimal')!==false) ||
							(strpos($meta->type, 'double')!==false) ||
							(strpos($meta->type, 'real')!==false) );
	}
}

// default callback
function _normal_output($s) { echo $s; }

function get_create_statement(&$db, $table_name, $outf, &$table_meta) {
	// create table query
	$outf("CREATE TABLE $table_name (\r\n");
//	$outf("id INTEGER AUTO_INCREMENT");
	if ($table_meta) {
		$keys = array_keys($table_meta);
		foreach ($keys as $k) {
			$meta =& $table_meta[$k];
			_meta_numeric_fix($meta);

/*			if( $meta->name=="id"
//				|| $key=="lastupd"
				) continue ; */

			$outf(",\r\n".$meta->name.' ');
			$outf(strtoupper($meta->type));
			if (($meta->max_length>0) && ($meta->type!=='int')) //skip display lengths
				$outf("(".$meta->max_length.")");
			if($meta->not_null) $outf(" NOT NULL");
			// to overcome 'mysql, mysqli' binding bugs
			if($meta->has_default && $meta->type != 'text' && $meta->type != 'longtext') {
				$outf(" DEFAULT ");
				if ($meta->numeric) {
					$def = (string)$meta->default_value;
					if (!strlen($def)) $def='0';
					$outf($def);
				} else {
					$outf("'".dkdb_encode($meta->default_value)."'");
				}
			}
		}
	}
//	$outf(",\nPRIMARY KEY (id) \r\n);\r\n\r\n");
	$outf("\r\n);\r\n\r\n");
}

function get_insert_statements(&$db, $table_name, $outf, &$table_meta) {
	$keys = array_keys($table_meta);
	$rsa=$db->GetArray('SELECT * FROM '.$table_name);
	foreach($rsa as $row) {
		$outf("INSERT INTO $table_name (");
		$c=count($row);
		for($i=0;$i<$c-1;$i++) {
//			if($keys[$i]=='lastupd')continue;
			$outf($table_meta[$keys[$i]]->name.',');
		}
		$outf($table_meta[$keys[$c-1]]->name);
		// removed newlines before VALUES for bug 573
		$outf(") VALUES (");
		reset($table_meta);
		for($i=0;$i<$c;$i++) {
//			if($keys[$i]=='lastupd')continue;
			$meta =& $table_meta[$keys[$i]];
			$val = $row[$meta->name];
			$sep = ($i==$c-1) ? '': ',';
			if (!$meta->numeric)
				$outf("'".dkdb_encode($val)."'".$sep);
			else {
				settype($val, 'string');
				$outf($val.$sep);
			}
		}
		$outf(");\r\n\r\n");
	}
}

header('Content-Type: text/plain');
$outf = '_normal_output';

// now create a MySQL-like dump
$tables = $db->MetaTables();
foreach ($tables as $table_name) {
	$outf("DROP TABLE $table_name;\r\n\r\n");
	$table_meta=$db->MetaColumns($table_name);
	get_create_statement($db, $table_name, $outf, $table_meta);
	get_insert_statements($db, $table_name, $outf, $table_meta);
}

?>