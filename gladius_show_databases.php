<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# SHOW DATABASES statement implementation
#


function _add_db(&$result, $dbname, $ver) {
	switch ($result->fetch_mode) {
		case GLADIUS_FETCH_BOTH:
			$record = array($dbname, $ver);
			$record['database'] =& $record[0];
			$record['version'] =& $record[1];
		break;
		case GLADIUS_FETCH_NUM:
			$record = array($dbname, $ver);
		break;
		default:
		case GLADIUS_FETCH_DEFAULT:
		case GLADIUS_FETCH_ASSOC:
			$record = array('database' => $dbname, 'version' => $ver);
	}
	$result->rowset[] = $record;
}

function _show_databases($g) {
	if (!($dj = opendir($g->db_root))) {
		$g->_error(_G__INVALID_DB_ROOT, $g->db_root);
		return false;
	}

	if (false ===($result = $g->_create_rs(array())))
		return false;

	while (false !== ($e = readdir($dj))) {
		if (($e=='.')||($e=='..')) continue;
		if (!is_dir($g->db_root.$e)) continue;
		if (!file_exists($g->db_root.$e.'/gladius.db.master.php')) {
		// also consider empty folders as databases
			_add_db($result, $e, '');
			continue;
		}
		$a = gladius_read($g->db_root.$e.'/gladius.db.master');

		_add_db($result, $e, Gladius::VersionString($a['GV']));
	}

	$result->EOR = true;
	$result->row_count = count($result->rowset);
	$result->aliases = array('database', 'version');
	$result->EOF = ($result->row_count == 0);

	$g->_success($result->row_count, 'database');

	return $result;
}

?>
