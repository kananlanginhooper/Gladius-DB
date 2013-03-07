<?php
## Gladius Database Engine
# @author legolas558
# @version 0.8.1
# Licensed under GNU General Public License (GPL)
#
#
# Test example
#

// deprecated since version 0.8.0
//$GLADIUS_DB_ROOT = './';

include '../gladius-testing.php';

$db->SetDBRoot(dirname(__FILE__).'/');

$sql = file_get_contents('gtestdb.sql');

gladius_test($sql);

?>
