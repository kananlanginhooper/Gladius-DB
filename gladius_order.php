<?php if(!defined('GLADIUS_HI_VER')){header('Status: 404 Not Found');die;}
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
# ordering functions
#

// main rowset ordering function
function rowsort(&$rs) {
	qsort_do($rs->rowset, 0, $rs->row_count - 1,
			 ($rs->order_way[0] == SORT_ASC ? 'ascsort' : 'descsort' ), $rs->order_ids[0]);
	if (count($rs->order_way) == 1)
		return;
	partsort($rs, 0, $rs->row_count);
}

// recursively sort using the next
function partsort_do(&$rs, $prev, $span) {
	$id = next($rs->order_ids);
	qsort_do($rs->rowset, $prev, $span, 
			($rs->order_way[key($rs->order_ids)] == SORT_ASC ? 'ascsort' : 'descsort' ),
			$id);
	if (key($rs->order_ids) != count($rs->order_ids)-1)
		partsort($rs, $prev , $span);
	prev($rs->order_ids);
}

function partsort(&$rs, $begin, $end) {
	$prev = 0;
	$span = 0;
	for($i=1;$i<$end;++$i) {
		if (equ_comp($rs->rowset[$i][current($rs->order_ids)],
					 $rs->rowset[$prev+$span][current($rs->order_ids)])) {
			++$span;
		} else {
			if ($span) {
				partsort_do($rs, $prev, $span);
				$span = 0;
			}
			$prev = $i;
		}
	}
	if ($span)
		partsort_do($rs, $prev, $span);
}

// recursive quicksort
function qsort_do(&$rows, $l, $r, $f, $col) {
    if ($l < $r) {
		qsort_partition($rows,$l,$r,$lp,$rp,$f, $col);
		qsort_do($rows,$l,$lp,$f, $col);
		qsort_do($rows,$rp,$r,$f, $col);
    }
}

// create the quicksort partition
function qsort_partition(&$rows,$l,$r,&$lp,&$rp,$f, $col) {
    $i = $l+1;
    $j = $l+1;
    while ($j <= $r) {
		if ($f($rows[$j][$col], $rows[$l][$col])) {
			$tmp = $rows[$j];
			$rows[$j] = $rows[$i];
			$rows[$i] = $tmp;
			++$i;
		}
		++$j;
    }
	$x = $rows[$l];
	$rows[$l] = $rows[$i-1];
	$rows[$i-1] = $x;
	$lp = $i - 2;
	$rp = $i;
}

// decremental sorting
function descsort(&$elem1, &$elem2) {
	return ($elem1 > $elem2);
}
//incremental sorting
function ascsort(&$elem1, &$elem2) {
	return ($elem1 < $elem2);
}
?>
