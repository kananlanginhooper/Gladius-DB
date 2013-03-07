<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# DROP DATABASE command execution
#

function _drop_database(&$conn, $database) {
	if ($database{strlen($database)-1}!='/')
		$database .= '/';

	$path = $conn->db_root.$database;
	$index = gladius_read($path.'gladius.db.master');
	if (null === $index) {
		$conn->_error(_G__DB_NOT_FOUND, $database);
		return false;
	}

	if ($database == $conn->database) {
		$conn->_reset();
		$conn->_command = 'DROP';
	}

	foreach ($index['TABLES'] as $table) {
		$schema = gladius_read($path.$table);
		if (!_drop_table($path, $table, $schema, $conn))
			return false;
	}

	@unlink($path.'gladius.txt');
	
	if ((false === @unlink($path.'gladius.db.master.php')) ||
		(false == @rmdir($path)) ) {
		$conn->_error(_G__REMOVE_ERROR, $database);
		return false;
	}
	
	$conn->_success(count($index['TABLES']), 'table');
	return true;
}

?>
