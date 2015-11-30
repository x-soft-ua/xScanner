<?php

require_once "./xScanner.class.php";
require_once "./analyzeTable.class.php";

class MyScanner extends xScanner
{

    protected static $redisCfg = [
        'redis_stat_a' => [
                                'count' => 5,
                                'point' => ['/tmp/redis_stat_a_{id}.sock']
                            ],
        'redis_stat_b' => [
                                'count' => 1,
                                'point' => ['127.0.0.1', '6379']
                            ],
    ];
    
    protected static $aerospikeCfg = [
        'db' => [
            'host' => '127.0.0.1',
        ]
    ];
    

    protected static $repository = ['getStatA' => [
                                                        'redis' => 'redis_stat_a',
                                                        'hashKey' => 'custom:Logs:',
                                                        'asCache' => [
                                                                        'connectName' => 'db',
                                                                        'ns' => 'ns1',
                                                                        'set' => 'temp',
                                                                        'ttl' => 180                //days
                                                                    ],
                                                        'keyStruct' => [                                  
                                                                            'type' => 0,           //1
                                                                            'uid' => 0,           //2
                                                                            'bid' => 0,           //3
                                                                            'aid' => 0,           //4
                                                                            'country' => 0,       //5
                                                                            'city' => 0,          //6
                                                                    ],
                                                        'argsPrior' => [
                                                                        ['uid', 'aid', 'bid'],
                                                                    ],
                                                        'availablePeriods' => ['d', 'm', 'w'],
                                                        ],
                                    'getStatB' => [
                                                        'redis' => 'redis_stat_b',
                                                        'hashKey' => 'custom:Logs2:',
                                                        'asCache' => [
                                                                        'connectName' => 'db',
                                                                        'ns' => 'ns1',
                                                                        'set' => 'temp',
                                                                        'ttl' => 180                //days
                                                                    ],
                                                        'keyStruct' => [                                  
                                                                            'type' => 0,           //1
                                                                            'uid' => 0,           //2
                                                                            'bid' => 0,           //3
                                                                            'aid' => 0,           //4
                                                                            'country' => 0,       //5
                                                                            'city' => 0,          //6
                                                                    ],
                                                        'argsPrior' => [
                                                                        ['uid', 'aid', 'bid'],
                                                                    ],
                                                        'availablePeriods' => ['d', 'm', 'w'],
                                                        ],                                                       
                                   ];
    
    public static $allowedMethods = array(
        'getStatA',
        'getStatB',
        'getAsyncWork',
        'initAsyncWork',
        'asyncWorkWatchdog',
    );
    
    
    public static function getStatA(&$result)
    {
        self::initStat($result, 'getStatA', 'c');
        $result = json_encode($result);
        return true;
    }
    public static function getStatB(&$result)
    {
        self::initStat($result, 'getPlannerStat', 'c');
        $result = json_encode($result);
        return true;
    }
    
}

?>