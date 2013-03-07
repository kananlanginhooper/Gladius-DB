<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# RENAME TABLE statement implementation
#
	if (!preg_match('/\\s+table\\s+/iA', $sql, $m, PREG_OFFSET_CAPTURE, $m[1][1]+6)) {
		$this->_error(_G__MALFORMED_SQL);
		$result = false;
		return;
	}
	$this->offset = $m[0][1]+strlen($m[0][0]);
	$from = array();
	$to = array();
	while (preg_match('/(\\w+)\\s+to\\s+(\\w+)\\s*(,)?\\s*/iA', $sql, $m, PREG_OFFSET_CAPTURE, $this->offset)) {
		$from_table = gladius_strtoupper($m[1][0]);
		$to_table =  gladius_strtoupper($m[2][0]);
		$this->offset = $m[0][1]+strlen($m[0][0]);

		$schema =& $this->_pick_schema($from_table);
		if ($schema === null) {
			$result = false;
			return;
		}
		
		if ($this->_exists($to_table)) {
			$this->_error(_G__TABLE_EXISTS, $to_table);
			$result = false;
			return;
		}

		$fields = array_keys($schema['FIELD_DEF']);
		foreach ($fields as $field) {
			$schema['FIELD_DEF'][$field]->table = $to_table;
			rename( $this->db_root.$this->database.$from_table.'.'.$field.'.php',
					$this->db_root.$this->database.$to_table.'.'.$field.'.php');
			if (isset($this->pool[$from_table])) {
				$this->pool[$to_table] =& $this->pool[$from_table];
				unset($this->pool[$from_table]);
			}
		}
		unlink($this->db_root.$this->database.$from_table.'.php');
		if (!$this->_save($to_table, $schema)) {
			$this->_error(_G__WRITE_FAILURE, $to_table);
			$result = false;
			return;
		}

		$this->schemas[$to_table] =& $schema;
		unset($this->schemas[$from_table]);
		$from[] = $from_table;
		$to[] = $to_table;
		if (!isset($m[3]))
			break;
	}
	
	$this->_build_index();
	
	$from = array_flip($from);
	$tables =& $this->index['TABLES'];
	$c=count($tables);
	for($i=0;$i<$c;$i++) {
		if (isset($from[$tables[$i]]))
			$tables[$i] = $to[$from[$tables[$i]]];
	}
	
	if (!$this->_save('gladius.db.master', $this->index)) {
		$this->_error(_G__WRITE_FAILURE, 'gladius.db.master');
		$result = false;
		return;
	}
	
	$this->_success(count($to), 'table');
	$result = true;
?>
