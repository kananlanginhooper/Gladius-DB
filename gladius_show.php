<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# SHOW command interpretation
#
	$this->offset=$m[0][1]+strlen($m[0][0]);
	if (!preg_match('/\\s*tables/iA', $sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
		if (preg_match('/\\s*databases/iA', $sql, $m,
			PREG_OFFSET_CAPTURE, $this->offset)) {
				$this->offset=$m[0][1]+strlen($m[0][0]);
				include_once GLADIUS_DIR.'gladius_show_databases.php';
				$result = _show_databases($this);
				return;
		}
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}
	$this->offset=$m[0][1]+strlen($m[0][0]);

	if (false ===($result = $this->_create_rs(array())))
		return;

	$this->_build_index();

	foreach ($this->index['TABLES'] as $table) {
		$schema =& $this->_pick_schema($table);
		if ($schema === null) {
			$result = false;
			return;
		}
		$ver = Gladius::VersionString($schema['GV']);
		switch ($result->fetch_mode) {
			case GLADIUS_FETCH_BOTH:
				$record = array($table, $ver, count($schema['FIELD_DEF']), $schema['TOP_INSERT_ID']);
				$record['table'] =& $record[0];
				$record['version'] =& $record[1];
				$record['fields'] =& $record[2];
				$record['top_insert_id'] =& $record[3];
			break;
			case GLADIUS_FETCH_NUM:
				$record = array($table, $ver, count($schema['FIELD_DEF']), $schema['TOP_INSERT_ID']);
			break;
			default:
			case GLADIUS_FETCH_DEFAULT:
			case GLADIUS_FETCH_ASSOC:
				$record = array('table' => $table, 'version' => $ver, 'fields' => count($schema['FIELD_DEF']), 'top_insert_id' => $schema['TOP_INSERT_ID']);
		}
		$result->rowset[] = $record;
	}

	$result->EOR = true;
	$result->row_count = count($result->rowset);
	$result->aliases = array('table', 'version', 'fields', 'top_insert_id');
	$result->EOF = ($result->row_count == 0);

	$this->_success($result->row_count, 'table');
?>
