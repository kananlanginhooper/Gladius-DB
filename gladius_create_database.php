<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# CREATE DATABASE command execution
#

function _create_database(&$conn, $database) {
	$path = $conn->db_root.$database;
	if ($database{strlen($database)-1}!='/')
		$path.='/';
	if (file_exists($path)) {
		$conn->_error(_G__DB_ALREADY_EXISTS, $database);
		return false;
	}
	if (FALSE === @mkdir($path)) {
		$conn->_error(_G__CANNOT_CREATE_DB, $database);
		return false;
	}
	
	$blank_db = array(
		'GV' => (int)((GLADIUS_HI_VER << 16) | (GLADIUS_LO_VER)),
		'TABLES' => array()
	);
	$conn->index['TABLES'] = array();

	gladius_write($path.'gladius.db.master', $blank_db);

	$f = fopen($path.'/gladius.txt', 'w');
	fwrite($f, '

Gladius v'.GLADIUS_HI_VER.'.'.GLADIUS_LO_VER.' database folder

This folder contains data files (saved with .php extension) managed by
the Gladius DB engine. Manual editing of these files may result in data
corruption.

For more information about Gladius DB visit

http://gladius.sourceforge.net/

'.chr(26));
	fclose($f);

	$conn->_success(1, 'database');
	return true;
}

?>
