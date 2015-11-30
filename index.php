<?php

    require_once './myScanner.class.php';
    
    if(php_sapi_name() == "cli")
    {
        $_GET['method'] = !empty($argv[1]) ? $argv[1] : '';
        MyScanner::$argv = !empty($argv) ? $argv : [];
    }
    
    if(empty($_GET['method']) ||
       !MyScanner::checkMethod($_GET['method']))
    {
        echo '{"error": "method_error"}';
        die();
    }
    
    $method = $_GET['method'];
    
    $sort = !empty($_GET['sort']) &&
                ($_GET['sort'] == 'desc' ||
                 $_GET['sort'] == 'asc') ? [$_GET['sort'] => -1] : [];
    

    if(MyScanner::$method($result))
    {
        if(isset($_GET['human']))
        {
            header('Content-Type: text/html');
            $o = new analyzeTable();
            $html = $o->getTable(json_decode($result, true), $sort);
            echo $html;
        }
        else
        {
            header('Content-Type: text/plain');
            echo $result;
        }
    }
  
?>