<?php
/**
 * Created by PhpStorm.
 * User: snail
 * Date: 2017/8/3
 * Time: 下午3:10
 */

namespace com\pindakong\tp5ext\driver\cache;

use think\cache\Driver;

/**使用phpredis
 * https://github.com/phpredis/phpredis/blob/feature/redis_cluster/cluster.markdown
 * Class RedisCluster
 *
 * @package com\pindakong\tp5ext\driver\cache
 */
class RedisCluster extends Driver
{
    /**
     * @var array
     */
    protected $options = [
        //主机或IP，如192.168.0.110,192.168.0.110
        'host'                => '127.0.0.1',//slave host
        //端口，如7001,7002,7003
        'port'                => 6379,
        //链接超时时间
        'timeout'             => 1.5,
        //读取超时时间
        'read_timeout'        => 1.5,
        //过期时间
        'expire'              => 0,
        //持久化链接
        'persistent'          => false,
        //缓存前缀
        'prefix'              => '',
        //是否需要json序列化
        'serialize'           => true,
        // 是否需要断线重连
        'break_reconnect'     => true,
        //最大重试连接次数
        'max_reconnect_times' => 20,
    ];

    /**
     * @var array
     */
    private static $node_params = [];

    /**需要重连的错误信息
     *
     * @var array
     */
    private $breakMatchStr = [
        'Connection refused',
    ];

    /**
     * 架构函数
     *
     * @access public
     *
     * @param  array $options 缓存参数
     *
     * @throws \Exception
     */
    public function __construct($options = [])
    {
        if (!extension_loaded('redis'))
        {
            throw new \BadFunctionCallException('not support: redis');
        }

        if (!empty($options))
        {
            $this->options = array_merge($this->options, $options);
        }
        $this->init_connect();
    }

    /**
     * @param int $reconnect_times 重连次数
     *
     * @throws \Exception
     */
    protected function init_connect($reconnect_times = 0)
    {
        //此处进行分布式配置
        $params = array(
            'hosts' => explode(',', $this->options['host']),
            'ports' => explode(',', $this->options['port']),
        );
        //拼接参数
        $hostsNum = count($params['hosts']);
        $seeds = [];
        for ($i = 0; $i < $hostsNum; $i++)
        {
            $host = $params['hosts'][$i];
            $port = $params['ports'][$i] ? $params['ports'][$i] : $params['ports'][0];
            $seeds[$i] = $host . ":" . $port;
        }
        try
        {
            //连接并指定timeout和read_timeout
            $this->handler = new \RedisCluster(NULL, $seeds, $this->options["timeout"], $this->options["read_timeout"], $this->options["persistent"]);
            // 始终在主机和从机之间随机分配只读命令
            $this->handler->setOption(\RedisCluster::OPT_SLAVE_FAILOVER, \RedisCluster::FAILOVER_DISTRIBUTE);
        }
        catch (\Exception $e)
        {
            if ($this->isBreak($e))
            {
                if ($reconnect_times <= $this->options['max_reconnect_times'])
                {
                    echo $reconnect_times . "<br/>";
                    sleep(0.5);
                    $this->init_connect(++$reconnect_times);
                }
                else
                {
                    throw $e;
                }
            }
            else
            {
                throw $e;
            }
        }
    }

    /**是否断线
     *
     * @param \Exception $e
     *
     * @return bool
     */
    protected function isBreak(\Exception $e)
    {
        if (!$this->options['break_reconnect'])
        {
            return false;
        }

        $error = $e->getMessage();

        foreach ($this->breakMatchStr as $msg)
        {
            if (false !== stripos($error, $msg))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断缓存是否存在
     *
     * @access public
     *
     * @param  string $name 缓存变量名
     *
     * @return bool
     */
    public function has($name)
    {
        // TODO: Implement has() method.
        return $this->handler->exists($this->getCacheKey($name));
    }

    /**
     * 读取缓存
     *
     * @access public
     *
     * @param  string $name    缓存变量名
     * @param  mixed  $default 默认值
     *
     * @return mixed
     */
    public function get($name, $default = false)
    {
        // TODO: Implement get() method.
        $this->readTimes++;

        $value = $this->handler->get($this->getCacheKey($name));

        if (is_null($value) || false === $value)
        {
            return $default;
        }

        return $this->unserialize($value);
    }

    /**
     * 写入缓存
     *
     * @access public
     *
     * @param  string $name   缓存变量名
     * @param  mixed  $value  存储数据
     * @param  int    $expire 有效时间 0为永久
     *
     * @return boolean
     */
    public function set($name, $value, $expire = null)
    {
        // TODO: Implement set() method.
        $this->writeTimes++;

        if (is_null($expire))
        {
            $expire = $this->options['expire'];
        }

        if ($this->tag && !$this->has($name))
        {
            $first = true;
        }

        $key = $this->getCacheKey($name);
        $expire = $this->getExpireTime($expire);

        $value = $this->serialize($value);

        if ($expire)
        {
            $result = $this->handler->setex($key, $expire, $value);
        }
        else
        {
            $result = $this->handler->set($key, $value);
        }

        isset($first) && $this->setTagItem($key);

        return $result;
    }

    /**
     * 自增缓存（针对数值缓存）
     *
     * @access public
     *
     * @param  string $name 缓存变量名
     * @param  int    $step 步长
     *
     * @return false|int
     */
    public function inc($name, $step = 1)
    {
        // TODO: Implement inc() method.
        $this->writeTimes++;

        $key = $this->getCacheKey($name);

        return $this->handler->incrby($key, $step);
    }

    /**
     * 自减缓存（针对数值缓存）
     *
     * @access public
     *
     * @param  string $name 缓存变量名
     * @param  int    $step 步长
     *
     * @return false|int
     */
    public function dec($name, $step = 1)
    {
        // TODO: Implement dec() method.
        $this->writeTimes++;

        $key = $this->getCacheKey($name);

        return $this->handler->decrby($key, $step);
    }

    /**
     * 删除缓存
     *
     * @access public
     *
     * @param  string $name 缓存变量名
     *
     * @return boolean
     */
    public function rm($name)
    {
        // TODO: Implement rm() method.
        $this->writeTimes++;

        return $this->handler->del($this->getCacheKey($name));
    }

    /**
     * 清除缓存
     *
     * @access public
     *
     * @param  string $tag 标签名
     *
     * @return boolean
     */
    public function clear($tag = null)
    {
        // TODO: Implement clear() method.

        if ($tag)
        {
            // 指定标签清除
            $keys = $this->getTagItem($tag);

            foreach ($keys as $key)
            {
                $this->handler->del($key);
            }

            $this->rm('tag_' . md5($tag));
            return true;
        }

        $this->writeTimes++;

        return $this->handler->flushDB(self::$node_params);
    }

    /**
     * 序列化数据
     *
     * @access protected
     *
     * @param  mixed $data
     *
     * @return string
     */
    protected function serialize($data)
    {
        if (is_scalar($data) || !$this->options['serialize'])
        {
            return $data;
        }

        return self::$serialize[2] . json_encode($data);
    }

    /**
     * 反序列化数据
     *
     * @access protected
     *
     * @param  string $data
     *
     * @return mixed
     */
    protected function unserialize($data)
    {
        if ($this->options['serialize'] && 0 === strpos($data, self::$serialize[2]))
        {
            return json_decode((substr($data, self::$serialize[3])), true);
        }
        else
        {
            return $data;
        }
    }
}