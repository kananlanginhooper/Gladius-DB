<?php
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# Testing include file
#

include 'gladius.php';

$db = new Gladius();

// used to debug a WHERE expression
function debug_chain(&$exp, $is_val = false) {
	if (isset($exp->bool_op))
		echo '<strong style="color:red">'.gladius_strtoupper($exp->bool_op).'</strong> ';
	else if (!$is_val) echo '<big>[</big>';
	if (isset($exp->op)) {
		debug_chain($exp->left, true);
		echo ' <strong>'.gladius_strtoupper($exp->op).'</strong> ';
		debug_chain($exp->right, true);
		if (!$is_val)
			echo '<big>]</big>';
	} else {
		echo get_class($exp).(isset($exp->content) ? '('.$exp->content.')' : '');
	}
	if (isset($exp->next)) {
		debug_chain($exp->next);
		echo '<big>]</big>';
	}
}

function gladius_test(&$sql) {
	$err = 0;

	function split_sql(&$sql) {
		// remove the single-line and multi-line comments
		$sql = preg_replace('/\\/\\*.*?\\*\\//s', '', $sql);
	/*	some history of PHP here. See http://bugs.php.net/bug.php?id=41050
		Before the integration of PCRE 7.0 into PHP (e.g. PHP < 4.4.6 & PHP <= 5.2.0)
		the following regular expression worked with no harm for nobody:
		/(insert|drop|create|select|delete|update|use|rename)([^;']*(('[^']*')+)?)*(;|$)/i
		when PCRE 7.0 was inserted, it stopped working (causing a segmentation fault)
	*/
	if (preg_match_all("/(?i:select|update|insert|delete|create|drop|use|rename|show)\\s+([^;']*('[^']*')*)*?(;|$)/",$sql, $m))
			return $m[0];
		else
			return array();
	}

	$queries = split_sql($sql);
	
	global $db;
	foreach ($queries as $sql) {
	$sql = trim($sql);
	$time_start = array_sum(explode(' ', microtime()));
	echo'<pre style="background-color: black; color: cyan">'.$sql.'</pre>';
	$rs = $db->query($sql);
	if (is_bool($rs)) {
		if (!$rs) {
			$err++;
			$style = ' style="color:red"';
		} else
			$style = ' style="color:lime"';
	} else $style = '';

	echo "<p$style>".$db->errstr.' in '.(array_sum(explode(' ', microtime())) - $time_start).' s</p>';
	if (is_bool($rs))
		continue;
		
	$heads = $rs->Fields();
	?><table width="100%" border="1" cellspacing="0"><tr><?php
		foreach($heads as $head)
			echo '<td><b>'.$head.'</b></td>';
		echo '</tr>';

		$antifreeze = 5000;
		$rsa = $rs->GetArray();
		foreach($rsa as $row) {

			if ($antifreeze-- < 0) {
				echo '<h1>Execution frozen!</h1><p>This may be due to a bug in the implementation</p>';
				exit;
			}

			echo '<tr>';
			foreach ($row as $field) {
				echo '<td>';var_dump($field); echo'</td>';
			}
			echo '</tr>';
		}
		echo '</table><br><br>';
	}
	return $err;
}

?>
