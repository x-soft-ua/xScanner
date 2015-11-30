<?php

abstract class xScanner
{
    
    const DEFAULT_SEPARATOR = ':';
    const DEFAULT_SEPARATOR20 = '::';
    
    const WORK_IN_PROGRESS = 0;
    const WORK_COMPLETE = 1;
    const WORK_ERR = 2;
    
    //in hours
    const RESULT_LIFE_TIME = 48;
    
    /*
     * Settings
     */
    protected static $redisCfg = [];
    protected static $aerospikeCfg = [];
    protected static $repository = [];
    
    
    public static $argv = [];
    public static $get_args = [];
    public static $post_args = [];
    
    private static $mapReduceBuf = [];
    
    
    private static $useVersion1 = false;
    private static $isDebug = false;
    private static $connections = [];
    private static $prefs = [];
    
    private static $currentMethod = '';
    private static $currentSeparator = '';
    
    //Список методов
    public static $allowedMethods = [];
    
    
    /**
     * Exceptions
     */
    private static function genException($msg = '')
    {
        throw new Exception($msg);
        return true;
    }
    
    /**
     * Устанавливет разделитель ключа для сканнера
     */
    private static function setReduceSeparator()
    {
        self::$currentSeparator = self::DEFAULT_SEPARATOR;
        
        if(!self::$useVersion1)
            self::$currentSeparator = self::DEFAULT_SEPARATOR20;
            
            
        return true;
    }
    
    /**
     * Вспомогательный метод получения точек подключения
     */
    private static function getRedisConnPoints($name, $count = 1)
    {
        
        if(empty($name))
            return [[], 0];
        $node = 0;
        
        if(empty($count) ||
           !isset(static::$redisCfg[$name]['count']) ||
           $count > static::$redisCfg[$name]['count'] ||
           !isset(static::$redisCfg[$name]['point']))
            return [[], 0];
    

        $ret = [];
        while($node < $count)
        {
            $cfg = static::$redisCfg[$name]['point'];

            if(count($cfg)==1)
            {
                $point = 'unix://'.$cfg[0];
                $point = str_replace('{id}', $node, $point);
            }
            else
            {
                $point = 'tcp://'.$cfg[0].':'.(intval($cfg[1])+$node);
            }
            $ret[] = $point;
            $node++;
        }
        
        $correction_factor = static::$redisCfg[$name]['count']/count($ret);
       
        return [$ret, $correction_factor];
    }
    
    /**
     * Адаптирует данные для intersect-паттерна
     */
    private static function checkIntersectPattern(&$ret, $el, $explodeEl = false)
    {
        if(!$explodeEl && !is_array($el))
            return false;
        
        if($explodeEl)
            $el = explode(',', $el);
            
        $el = array_keys(array_flip($el));
        sort($el);
        $ret = '*,'.implode(',*,', $el).',*';
        
        return true;
    }
    
    /**
     * Метод рекурсивного анализа комбинированных значений
     */
    private static function &getRecursiveCursor(&$cursorStatus, &$result, $resultStruct, $memsExploded, $explStruct = [])
    {
        //self::$mapReduceBuf
        $structPointer =& $result;

        foreach($resultStruct as $elId)
        {

            $exKey = explode('_:', $memsExploded[$elId]);
            if(count($exKey)==2)
                $memsExploded[$elId] = $exKey[1];
          
            $elKey = $memsExploded[$elId];
                
            /**
             * Если нашли эллемент-массив
             */
            if(isset($explStruct[$elId]) && !isset(self::$mapReduceBuf[$elId]))
            {
                self::$mapReduceBuf[$elId] = [];
                self::$mapReduceBuf[$elId]['el'] = explode(',,', trim($elKey, ','));
                self::$mapReduceBuf[$elId]['pos'] = 0;
                
                $elKey = self::$mapReduceBuf[$elId]['el'][0];
            }
            elseif(isset($explStruct[$elId]))
            {
                self::$mapReduceBuf[$elId]['pos']++;
                $elKey = self::$mapReduceBuf[$elId]['el'][self::$mapReduceBuf[$elId]['pos']];
            }
            /**
             */
            
            if(!isset($structPointer[$elKey]))
                $structPointer[$elKey] = array();
            $structPointer =& $structPointer[$elKey];
        }
        
        $cursorStatus = false;
        return $structPointer;
    }
    
    /**
     * Строит правильный список значений для зависимых фильтров,
     * в зависимости от входных параметров
     *
     *  @getArgs - массив входных параметров
     *  @argsPrior - массив с приоритетами фильтров
     */
    private static function getDependentGetFilters($getArgs = [], $argsPrior = [])
    {
        $result = [];
        foreach($argsPrior as $iteration)
        {
            if(!is_array($iteration))
                continue;
            
            $filter = '*';
            foreach($iteration as $element)
            {
                if(!empty($getArgs[$element]))
                {
                    $filter = '0';
                    continue;
                }
                $result[$element] = !isset($result[$element]) ? $filter : $result[$element];
            }
        }
        return $result;
    }
    
    /*
     * Метод асинхронной агрегации данных
     * 
     *   @current_key    - сканируемый ключ
     *   @current_filter - паттерн
     *   @resultStruct         - связка элементов после анализа ключа:
     *                       @elementsMatch = array(0,1,4,2)
     */
    private static function initMapReduceStat(&$result,
                                              $redisList,
                                              $counterName,
                                              $current_key,
                                              $current_filter = '*',
                                              $resultStruct = array(),
                                              $excludedMems = array(),
                                              $correction_factor = 1)
    {

        /**
         * Обновленная структура 2.0
         */
        
        $explStruct = [];
        if(isset($resultStruct['v2']['struct']))
        {
            $explStruct = array_flip($resultStruct['v2']['expl']);
            $resultStruct = $resultStruct['v2']['struct'];
        }


        if(empty($resultStruct) || empty($counterName))
            return false;
        
        //echo $current_filter.PHP_EOL;
        //$redisToScan = array('unix:///tmp/redis_xmq_stat_0.sock',
        //                     'unix:///tmp/redis_xmq_stat_1.sock',
        //                     'unix:///tmp/redis_xmq_stat_2.sock',
        //                     'unix:///tmp/redis_xmq_stat_3.sock',
        //                     'unix:///tmp/redis_xmq_stat_4.sock');

        if(!empty($redisList) && is_array($redisList))
        {
            $redisToScan = $redisList;
        }
        else
        {
            $c = self::getConnections();

            if(!isset($c[$redisList]) || !is_array($c[$redisList]))
                return false;
            $redisToScan = $c[$redisList];
        }
        

        
        $itArr = array();
        
        self::setReduceSeparator();
        
        if($redisList == 'redis_planner_stat_list')
        {
            //print_r($resultStruct);
            //die();
        }

        $scanCount = 1000;
        
        $tmp = 0;
        $c = 0;
        while(!empty($redisToScan))
        {
            sort($redisToScan);
            //$tmp++;
            //if($tmp>5)
            //{
            //    die();
            //}
            
            $ps = new xAsyncRedis();
            $ps->settimeout(10000);
            $arr_mems = false;
            $redisCommands = [];
            foreach($redisToScan as $hostId => $host)
            {
                $it = isset($itArr[$hostId]) ? $itArr[$hostId] : 0;
                $redisCommands[] = array("HSCAN $current_key $it MATCH $current_filter COUNT $scanCount", $host);
            }
            

            
            $ps->setCommands($redisCommands);
            $ps->run();
            $redisResponses = $ps->getresponse();


            foreach($redisResponses as $hostId => $hostResponse)
            {

                //Ошибка?
                if(empty($hostResponse) || !empty($hostResponse[1]))
                {
                    unset($redisToScan[$hostId]);
                }
                
                //Конец хеша?
                if(empty($hostResponse[0][0]))
                {
                    unset($redisToScan[$hostId]);
                }
                else
                    $itArr[$hostId] = intval($hostResponse[0][0]);
                
                if(!empty($hostResponse[0][1]) && is_array($hostResponse[0][1])) 
                {
                    $i = 0;
                    foreach($hostResponse[0][1] as $val)
                    {
                        if($i == 0)
                        {
                            $el = $val;
                            $i = 1;
                        }
                        else
                        {
                            //Временный фикс склеивания
                            $el = str_replace(':::', ':0::', $el);
                            
                            //Разбиваем ключ на части
                            $memsExploded = explode(self::$currentSeparator, $el);
                            
                            
                            $exclude = false;
                            foreach($excludedMems as $exMemId => $exMemArr)
                            {
                                //Если нашли исключение, пропускаем
                                if(isset($memsExploded[$exMemId]) &&
                                   isset($exMemArr[$memsExploded[$exMemId]]))
                                    $exclude = true;
                            }
                            if(!$exclude)
                            {
                                //Устанавливаем указатель на вложенный элемент
                                $structPointer =& $result;
                                
                                self::$mapReduceBuf = [];

                                
                                //foreach($resultStruct as $elId)
                                //{
                                //
                                //    /**
                                //     * Separator v2.0
                                //     */
                                //    $exKey = explode('_:', $memsExploded[$elId]);
                                //    if(count($exKey)==2)
                                //        $memsExploded[$elId] = $exKey[1];
                                //  
                                //    $elKey = $memsExploded[$elId];
                                //        
                                //    if(!isset($structPointer[$elKey]))
                                //        $structPointer[$elKey] = array();
                                //    $structPointer =& $structPointer[$elKey];
                                //}
                                
                                $cursorStatus = true;
                                do
                                {
                                    $structPointer =& self::getRecursiveCursor($cursorStatus, $result, $resultStruct, $memsExploded, $explStruct);
                                    
                                    $c = $c  + $val;
                                    if(isset($structPointer))
                                    {
                                        $structPointer[$counterName] = !isset($structPointer[$counterName]) ? ($val * $correction_factor) : $structPointer[$counterName] + ($val * $correction_factor);
                                    }
                                }
                                while($cursorStatus);
                                
                                
                            }
                            
                            $i = 0;
                            
                        }
                        
                    }
                }
            }
            

        }
    }
    
    
    /**
     * Метод-шаблон для запуска сканнера статистики
     *
     *  @result
     *  @statMethod
     *  @counterName
     */
    protected static function initStat(&$result, $statMethod = '', $conterName = 'c')
    {
        
        $sumTimeGen = !empty($result['timeGen']) ? $result['timeGen'] : 0;
        $result = !empty($result['via']) && is_array($result['via']) ? $result['via'] : [];
        
        extract(self::getConnections());

        if(empty(static::$repository[$statMethod]))
            self::genException('empty_repository');
            
        try
        {
            //AsyncWorker
            if(!empty(self::$get_args))
                $_GET = self::$get_args;
                
            $requestId = md5($_SERVER['REQUEST_URI']);
            $startMicro = microtime(1);
            $requestUriId = sha1($_SERVER['REQUEST_URI']);
       
            //Срезы данных        
            $viaIdents = [];
            

            $viaZeroArr = array();
            
            /**
             * Охват данных, расчитываем скольо редисов использовать
             */
            $scan_width = !empty($_GET['scan_width']) ? intval($_GET['scan_width']) : 1;

            if($scan_width < 1 || $scan_width > 5)
                $scan_width = 1;
            list($redisPonits, $correction_factor) = self::getRedisConnPoints(static::$repository[$statMethod]['redis'], ($scan_width));


    
            /**
             * Готовим фильтр
             * 
             * Структура фильтра:
             *  0 - обычный фильтр
             *  1 - пересекающийся фильтр
             */
            $keyStruct = static::$repository[$statMethod]['keyStruct'];
            
            /**
             * Аргументы имеющие приоритетную связь.
             * Если задан, то правее его = '*', левее = '0'
             */
            $argsPrior = static::$repository[$statMethod]['argsPrior'];
            
            
            $defaultArgs = self::getDependentGetFilters($_GET, $argsPrior);
            

            $inputFilters = [];
            $unionFilters = [];
            foreach($keyStruct as $argName => $argType)
            {
                if(isset($_GET[$argName]) && strpos($_GET[$argName], ',,')!==false)
                {
                    $unionFilters[] = explode(',,',$_GET[$argName]);
                    $inputFilters[] = isset($defaultArgs[$argName]) ? $defaultArgs[$argName] : '*';

                }
                else
                {
                    $unionFilters[] = null;
                    if($argType==1)
                        $inputFilters[] = isset($_GET[$argName]) && strlen(strval($_GET[$argName]))>0 &&
                                            self::checkIntersectPattern($r, $_GET[$argName], true) ? $r : (isset($defaultArgs[$argName]) ? $defaultArgs[$argName] : '*');
                    else
                        $inputFilters[] = isset($_GET[$argName]) && strlen(strval($_GET[$argName]))>0 ? trim($_GET[$argName]) : (isset($defaultArgs[$argName]) ? $defaultArgs[$argName] : '*');
                }
            }


            
            $current_filter = implode(':', $inputFilters);
            
            self::adaptKey($current_filter);
            /**
             */
            
            /**
             * Динамический срез via
             */
            if(!empty($_GET['viacustom']))
            {
                $viaIdents['custom'] = [];
                $viacustom = explode(',', $_GET['viacustom']);
                
                foreach($viacustom as $row)
                {
                    if(isset($keyStruct[$row])) //&& empty($keyStruct[$row]))
                    {
                        $viaIdents['custom'][] = $row;  
                    }
                }
                if(!empty($viaIdents['custom']))
                    $_GET['via'] = 'custom';        
            }


            $via = !empty($_GET['via']) && isset($viaIdents[$_GET['via']]) ? $viaIdents[$_GET['via']] : 'def';
            $viaOrig = !empty($_GET['via']) && isset($viaIdents[$_GET['via']]) ? $_GET['via'] : 'def';
    
            $timeEnd = !empty($_GET['timeEnd']) ? intval($_GET['timeEnd']) : 0;
            $timeStart = !empty($_GET['timeStart']) ? intval($_GET['timeStart']) : 0;
            $period = !empty($_GET['period']) && in_array($_GET['period'], static::$repository[$statMethod]['availablePeriods']) ?
                            ($_GET['period']) : static::$repository[$statMethod]['availablePeriods'][0];

            
            if(empty($timeEnd) && $timeEnd > $timeStart)
                $timeEnd = $timeStart;
                
    
            /**
             * DEPRECATED:
             * Для некоторых множество нужно менять * на 0 в паттерне, чтобы исключить
             * подсет значений высшего уровня
             */
            //if(isset($viaZeroArr[$viaOrig]))
            //foreach($viaZeroArr[$viaOrig] as $elToZero)
            //{
            //    if($$elToZero == '*')
            //    {
            //        $$elToZero = '0';
            //        foreach($viaIdents[$viaOrig] as $k => $v)
            //            if($v==$elToZero)
            //                unset($viaIdents[$viaOrig][$k]);
            //    }
            //}
    
            /**
             * Формируем массив исключений
             */
            $excluded_arr = array();
            //$excluded_arr = $excluded_arr + array('1' => $ex_country_ids);
            
            /**
             * Начало подсчетов
             */

            //Проходим по каждому периоду отдельно
            while($timeStart>=0 && $timeStart>=$timeEnd)
            {
                //Сканируемый период
                $scanTime = self::getTime($period, -1 * $timeStart);
                
                /**
                 * Формируем ключ хеша
                 */
                $current_key = static::$repository[$statMethod]['hashKey'].$period.$scanTime;

                if(self::moveFromAs($current_key, $moveresult, $statMethod))
                    $current_key = 'cache_'.$current_key;
    
                
                //Определяем размещение элементов по ключу
                $callKeyStruct = array_flip(array_keys($keyStruct));
                
                //Формируем результирующую структуру согласно типа среза "via"
                $structToMapReduce = ['v2' => ['struct' => [], 'expl' => []]];


                if(isset($viaIdents[$viaOrig]) && is_array($viaIdents[$viaOrig]))
                    foreach($viaIdents[$viaOrig] as $viaEl)
                    {
                        
                        if(!empty($keyStruct[$viaEl]))
                            $structToMapReduce['v2']['expl'][] = $callKeyStruct[$viaEl];
                            
                        if(isset($callKeyStruct[$viaEl]))
                        $structToMapReduce['v2']['struct'][] = $callKeyStruct[$viaEl];
                    }


                /**
                 * Вызовы
                 */
    

                //$current_filter = '*';
                //echo $current_filter.PHP_EOL;
                //$current_filter = '*,1,*,2,*';
                self::$isDebug = true;
                //Считаем
                self::initMapReduceStat($result, $redisPonits, $conterName, $current_key, $current_filter, $structToMapReduce, $excluded_arr, $correction_factor);
                             
                //Следующий период
                $timeStart--;
            }

            ksort($result); 

            $result = array('status' => self::WORK_COMPLETE,
                            'via' => $result,
                              'timeGen' => (microtime(1) - $startMicro) + $sumTimeGen);
        }
        catch (Exception $e)
        {
            $result = array('status' => self::WORK_ERR,
                            'statusDescr' => $e->getMessage(),
                              'timeGen' => (microtime(1) - $startMicro) + $sumTimeGen);
        }

        return true;
    }
    
    /**
     * Получаем кешированные данные из Aerospike
     */
    public static function moveFromAs($getkey, &$result, $statMethod = '', $movettl = 300)
    {

        if(empty($getkey) ||
           empty($statMethod))
            return false;

        if(empty(static::$repository[$statMethod]))
            self::genException('empty_repository');
            
        //Получаем коннекты
        extract(self::getConnections());
        
        //Имя массива редисов
        $redisName = static::$repository[$statMethod]['redis'];

        //Количество НОД
        $nodesCount = (int)static::$redisCfg[$redisName]['count'];

        //Коннектор к аероспайку
        $dbConnectName = static::$repository[$statMethod]['asCache']['connectName'];
        

        if(empty($$dbConnectName))
            self::genException('aerospike_connector_not_set');        
        
        $dbConnect = $$dbConnectName;
        
        //Локация хранилища
        $nspace_name = static::$repository[$statMethod]['asCache']['ns'];
        $set_name = static::$repository[$statMethod]['asCache']['set'];
        $set_name = static::$repository[$statMethod]['asCache']['set'];


        $node = 0;
        while($node < $nodesCount)
        {
            $redisNode = $redisName.'_'.$node;

            if($$redisNode->exists($getkey))
                return false;
        
            if($$redisNode->exists('cache_'.$getkey))
                return true;
            


            /*
             * Берем по основному ключу список блоков
             */
            $key = $dbConnect->initKey($nspace_name, $set_name, $getkey.'_'.$node);
            $status = $dbConnect->get($key, $record);

            $data = [];
            
            if($status !== Aerospike::OK)
                return false;
            
            if(!isset($record['bins']['mems']))
                return false;
            
    
            //Читаем все блоки
            foreach($record['bins']['mems'] as $memKey)
            {
                $key = $dbConnect->initKey($nspace_name, $set_name, $memKey);
                $status = $dbConnect->get($key, $record);
                if($status !== Aerospike::OK)
                    return false;
                
                //Десериализируем
                if(isset($record['bins']['data']))
                    $record['bins']['data'] = json_decode($record['bins']['data'], true);
                
                //Складываем все блоки вооедино
                if(!empty($record['bins']['data']))
                    foreach($record['bins']['data'] as $key => $val)
                    {
                        $data[$key] = !isset($data[$key]) ?  0 : $data[$key];
                        $data[$key] += $val; 
                    }
            }
            //Пишем все это в редиску
            if(!empty($data))
            {
                $status = $$redisNode->hMSet('cache_'.$getkey, $data);
                
                if($status === true)
                    $$redisNode->expire('cache_'.$getkey, $movettl);
            }

            
            $node++;
        }
        //$result = $data;
        //print_r($result);
        
        return true;
    }
    
    
    /*
     * Перемещает хеш из памяти в хранилище
     */
    public static function moveToAs(&$result)
    {
        //Получаем коннекты
        extract(self::getConnections());
        
        $timeStart = microtime(1);
        $result = [];
        
        //Размер блока
        $scan_count = 1500;
        
        echo microtime(1);
        return;
    
        foreach(static::$repository as $name => $cfg)
        {
            if(!isset($cfg['asCache']))
                continue;
            
            //Коннектор к аероспайку
            $dbConnectName = $cfg['asCache']['connectName'];
            if(empty($$dbConnectName))
                self::genException('aerospike_connector_not_set');        
            $dbConnect = $$dbConnectName;
            
            //Пути хранения
            $nspace_name = $cfg['asCache']['ns'];
            $set_name = $cfg['asCache']['set'];
            $ttl = $cfg['asCache']['ttl'];
            
            //Источник
            $redisName = $cfg['redis'];
            //Основание хеша
            $hashName = $cfg['hashKey'];
            //Количество НОД
            $nodesCount = (int)static::$redisCfg[$redisName]['count'];
            //Статус
            $withSaveOkArr = [];
            $withSaveErrArr = [];
    
            $node = 0;
            while($node < $nodesCount)
            {
                $redisNode = $redisName.'_'.$node;
                //Получаем все доступные имена хешей
                $all = $$redisNode->keys($hashName.'*');    
                //Статус
                $withSaveOk = [];
                $withSaveErr = [];
        
                foreach($all as $thisKey)
                {
                    $it = NULL;           
                    $arr_mems = true;
                    $partId = 0;
                    $memsKeyArr = [];
                    
                    while($arr_mems !== false)
                    {
                        $partId++;
                        //Вытягиваем блок данных
                        $arr_mems = $$redisNode->hscan($thisKey, $it, '*', $scan_count);
                        $saveKey = $thisKey.'_'.$node.'_'.$partId;
                        
                        if($arr_mems===false)
                            continue;
                        
                        //Сериализуем
                        $data = json_encode($arr_mems);
    
                        
                        //Пишем в бекап
                        $key = array("ns" => (string)$nspace_name, "set" => (string)$set_name, "key" => (string)$saveKey);
                        $status = $db->put($key, array('data' => $data, 'reckey' => $saveKey), $ttl);
                        
                        if($status !== Aerospike::OK)
                            break;
                        
                        //Если все ок, добавляем этот блок в общий список
                        $memsKeyArr[] = $saveKey;
                    }
                    if($status !== Aerospike::OK)
                    {
                        $withSaveErr[] = array($thisKey, $memsKeyArr);
                        continue;
                    }
                    
                    $key = array("ns" => (string)$nspace_name, "set" => (string)$set_name, "key" => $thisKey.'_'.$node);
                    $status = $db->put($key, array('mems' => $memsKeyArr, 'reckey' => $thisKey.'_'.$node), $ttl);
                    
                    if($status !== Aerospike::OK)
                        $withSaveErr[] = array($thisKey.'_'.$node, $memsKeyArr);
                    else
                        $withSaveOk[] = $thisKey;
                }
        
                /*
                 * Удаляем устаревшие ключи кроме "актуальных"
                 */
                foreach($withSaveOk as $savedKey)
                {
                    $savedKeyArr = explode(':', $savedKey);
                    $identEl = count($savedKeyArr)-1;
                    
                    if($identElement<1)
                        continue;
                    
                    $ident = trim($savedKeyArr[$identEl]);
                    if(!isset($excludeIdents[$ident]))
                    {
                        $$redisNode->del($savedKey);
                    }
                }
                
                $withSaveOkArr[$node] = $withSaveOk;
                $withSaveErrArr[$node] = $withSaveErr;
                
                $node++; 
            }
            
        }
        
        echo 1;
        return;
        
    
        
        $fullTime = microtime(1)-$timeStart;
        
        $result = [];
        //$result = json_encode(array('ok' => $withSaveOkArr,
        //                'err' => $withSaveErrArr,
        //                'saveLen' => $saveLen,
        //                'time' => $fullTime));
        
        return true;
    }
    
    /**
     * Адаптирует ключ для записи в хеш
     */
    public static function adaptKey(&$key, $separator = ':')
    {
        $keyExploded = explode($separator, $key);
        if(count($keyExploded)>1)
        {
            $key = '';
            foreach($keyExploded as $i => $v)
            {
                if(!empty($key))
                    $key .= '::';
                
                if(empty($v))
                    $v = '0';
                    
                $key .= $i.'_:'.$v;
            }
            return true;
        }
        return false;
    }
    
    /**
     * Проверяет наличие сканнер-метода
     */
    public static function checkMethod($method)
    {
        self::$currentMethod = $method;
        return !in_array($method, static::$allowedMethods) ?
                    false :
                    true;
    }
    
    
    /*
     * Получает идентификатор дня/недели/месяца
     *
     * @ident - идентификатор периода
     * @n - параметр сдвига
     */
    public static function getTime($ident, $n = 0)
    {
        
        $idents = array('h' => date('HdmY', time() + $n * 3600),
                        'd' => date('dmY', time() + $n * 24 * 3600),
                        'w' => date('WY', time() + $n * 24 * 3600 * 7),
                        'm' => date('mY', time() + $n * 24 * 3600 * 30));
        return isset($idents[$ident]) ? $idents[$ident] : '';
    
    }
    
    /*
     * Получение коннектов к базам
     */   
    public static function getConnections()
    {
        if(!empty(self::$connections))
            return self::$connections;
        
            
        foreach(static::$aerospikeCfg as $asName => $cfg)
        {
            if(!is_array($cfg))
                continue;

            $config = [
              "hosts" => [
                [ "addr" => $cfg['host'], "port" => 3000 ]]];
            $opts = array(Aerospike::OPT_CONNECT_TIMEOUT => 10000, Aerospike::OPT_WRITE_TIMEOUT => 5000);
            self::$connections[$asName] = new Aerospike($config, true, $opts);
        }
        
        foreach(static::$redisCfg as $redisName => $cfg)
        {
            if(!is_array($cfg))
                continue;

            do
            {
                $cfg['count']--;
                $redisNameNode = $redisName.'_'.$cfg['count'];
                self::$connections[$redisNameNode] = new Redis();

                
                if(count($cfg['point'])!==2)
                    self::$connections[$redisNameNode]->pconnect(str_replace('{id}', $cfg['count'], $cfg['point'][0]));
                else
                    self::$connections[$redisNameNode]->pconnect($cfg['point'][0], ($cfg['point'][1]+$cfg['count']));
            }
            while($cfg['count']>0);
        }
        
        return self::$connections;
    }
    

}

class ProcCmd
{
    private static $pid;

    public static function run($cmd){
        $command = '/usr/bin/nohup '.$cmd.' > /dev/null 2>&1 & echo $!';
        //echo $command;
        exec($command ,$op);
        self::$pid = (int)$op[0];
        return self::$pid;
    }

    public static  function setPid($pid){
        self::$pid = $pid;
    }

    public static  function getPid(){
        return self::$pid;
    }

    public static  function status(){
        if(empty(self::$pid))
            return false;
        
        $command = 'ps -p '.self::$pid;
        exec($command,$op);
        if (!isset($op[1]))return false;
        else return true;
    }

    public static  function stop(){
        if(empty(self::$pid))
            return false;
        
        $command = 'kill '.self::$pid;
        exec($command);
        if (self::status() == false)return true;
        else return false;
    }
}


?>