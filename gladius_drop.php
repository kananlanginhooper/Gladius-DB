<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# DROP command interpretation
#
	if (!function_exists('_drop_table')) {
		function _drop_table($path, $table, &$schema, &$conn) {
			foreach ($schema['FIELD_DEF'] as $fo) {
				if (!gladius__remove($path.$table, $fo->name, $schema)) {
					$conn->_error(_G__REMOVE_ERROR, $table.'.'.$fo->name);
					return false;
				}
			}
			if (!@unlink($path.$table.'.php')) {
				$this->_error(_G__REMOVE_ERROR, $table);
				return false;
			}
			// remove from cache and pool
			unset($schema);
			unset($conn->schemas[$table]);
			unset($conn->pool[$table]);
			
			return true;
		}
	}

	$this->offset = $m[1][1]+4;
	if (!preg_match('/\\s*table\\s*(\\w+)\\s*(,)?/iA', $sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
		if (preg_match('/\\s+database\\s+(\\w+)/iA', $sql, $m,
			PREG_OFFSET_CAPTURE, $this->offset)) {
				$database = gladius_strtolower($m[1][0]);
				$this->offset = $m[1][1]+strlen($database);
				include_once GLADIUS_DIR.'gladius_drop_database.php';
				$result = _drop_database($this, $database);
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

	global $GLADIUS_WARNINGS_AS_ERRORS;
	$this->_build_index();
	$dropped = 0;
	do {
		$table = gladius_strtoupper($m[1][0]);
	
		if (!isset($this->schemas[$table])) {
			$this->schemas[$table] = $this->_load($table);
			if (!isset($this->schemas[$table])) {
				if ($GLADIUS_WARNINGS_AS_ERRORS) {
					$this->_error(_G__TABLE_NOT_FOUND, $table);
					$result = false;
					return;
				} else {
					$this->_warning(_G__TABLE_NOT_FOUND, $table);
					$skip = true;
				}
			} else $skip = false;
		} else $skip = false;
		
		if (!$skip) {
			if (!_drop_table($this->db_root.$this->database, $table, $this->schemas[$table], $this)) {
				$result = false;
				return;
			}

			$c = count($this->index['TABLES']);
			for ($i=0;$i<$c;++$i) {
				if ($this->index['TABLES'][$i]==$table) {
					array_splice($this->index['TABLES'], $i, 1);
					break;
				}
			}
			++$dropped;
		}
		
		$this->offset = $m[count($m)-1][1] + strlen($m[count($m)-1][0]);
		if ($m[count($m)-1][0] != ',') {
			if (!$this->_save('gladius.db.master', $this->index)) {
				$this->_error(_G__WRITE_FAILURE, 'gladius.db.master');
				$result = false;
				return;
			}
			$this->_success($dropped);
			$result = true;
			return;
		}
		
		$matches = preg_match('/\\s*(\\w+)\\s*(,)?/iA', $sql, $m, PREG_OFFSET_CAPTURE, $this->offset);
	} while ($matches);
	if (!$this->_save('gladius.db.master', $this->index)) {
		$this->_error(_G__WRITE_FAILURE, 'gladius.db.master');
		$result = false;
		return;
	}

	$this->_error(_G__MALFORMED_SQL);
	$result = false;
?>
