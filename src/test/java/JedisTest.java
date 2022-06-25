import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.resps.ScanResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Project name(项目名称)：Redis_bigKey_and_bigKey_scan
 * Package(包名): PACKAGE_NAME
 * Class(类名): JedisTest
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/6/25
 * Time(创建时间)： 11:34
 * Version(版本): 1.0
 * Description(描述)： 无
 */


public class JedisTest
{
    private Jedis jedis;

    /**
     * Sets up.
     */
    @BeforeEach
    void setUp()
    {
        // 1.建立连接
        jedis = new Jedis("127.0.0.1", 6379);
        // 2.设置密码
        jedis.auth("123456");
        // 3.选择库
        jedis.select(0);
    }

    /**
     * Ping.
     */
    @Test
    void ping()
    {
        String ping = jedis.ping();
        System.out.println(ping);
    }

    /**
     * Test hash.
     */
    @Test
    void testHash()
    {
        // 插入hash数据
        jedis.hset("user:1", "name", "Jack");
        jedis.hset("user:1", "age", "21");

        // 获取
        Map<String, String> map = jedis.hgetAll("user:1");
        System.out.println(map);
    }


    /**
     * The constant STR_MAX_LEN.
     */
    final static int STR_MAX_LEN = 5 * 1024;
    /**
     * The constant HASH_MAX_LEN.
     */
    final static int HASH_MAX_LEN = 500;

    /**
     * 大key扫描
     */
    @Test
    void testScan()
    {
        int maxLen = 0;
        long len = 0;

        String cursor = "0";
        do
        {
            // 扫描并获取一部分key
            ScanResult<String> result = jedis.scan(cursor);
            // 记录cursor
            cursor = result.getCursor();
            List<String> list = result.getResult();
            if (list == null || list.isEmpty())
            {
                break;
            }
            // 遍历
            for (String key : list)
            {
                // 判断key的类型
                String type = jedis.type(key);
                switch (type)
                {
                    case "string":
                        len = jedis.strlen(key);
                        maxLen = STR_MAX_LEN;
                        break;
                    case "hash":
                        len = jedis.hlen(key);
                        maxLen = HASH_MAX_LEN;
                        break;
                    case "list":
                        len = jedis.llen(key);
                        maxLen = HASH_MAX_LEN;
                        break;
                    case "set":
                        len = jedis.scard(key);
                        maxLen = HASH_MAX_LEN;
                        break;
                    case "zset":
                        len = jedis.zcard(key);
                        maxLen = HASH_MAX_LEN;
                        break;
                    default:
                        break;
                }
                if (len >= maxLen)
                {
                    System.out.printf("找到 big key : %s, 类型: %s, 长度或者大小: %d %n", key, type, len);
                }
            }
        }
        while (!cursor.equals("0"));
    }

    /**
     * Test set big key.
     */
    @Test
    void testSetBigKey()
    {
        Map<String, String> map = new HashMap<>();
        for (int i = 1; i <= 1000; i++)
        {
            map.put("hello_" + i, "world!");
        }
        jedis.hmset("m2", map);
    }

    /**
     * Test big hash.
     */
    @Test
    void testBigHash()
    {
        Map<String, String> map = new HashMap<>();
        for (int i = 1; i <= 100000; i++)
        {
            map.put("key_" + i, "value_" + i);
        }
        jedis.hmset("test:big:hash", map);
    }

    /**
     * Test big string.
     */
    @Test
    void testBigString()
    {
        for (int i = 1; i <= 100000; i++)
        {
            jedis.set("test:str:key_" + i, "value_" + i);
        }
    }

    /**
     * Test big string delete.
     */
    @Test
    void testBigStringDelete()
    {
        for (int i = 1; i <= 100000; i++)
        {
            jedis.del("test:str:key_" + i);
        }
    }

    /**
     * Test small hash.
     */
    @Test
    void testSmallHash()
    {
        int hashSize = 100;
        Map<String, String> map = new HashMap<>(hashSize);
        for (int i = 1; i <= 100000; i++)
        {
            int k = (i - 1) / hashSize;
            int v = i % hashSize;
            map.put("key_" + v, "value_" + v);
            if (v == 0)
            {
                jedis.hmset("test:small:hash_" + k, map);
            }
        }
    }

    /**
     * Test for.
     */
    @Test
    void testFor()
    {
        for (int i = 1; i <= 100000; i++)
        {
            jedis.set("test:key_" + i, "value_" + i);
        }
    }

    /**
     * Test mxx.
     */
    @Test
    void testMxx()
    {
        String[] arr = new String[2000];
        int j;
        long b = System.currentTimeMillis();
        for (int i = 1; i <= 100000; i++)
        {
            j = (i % 1000) << 1;
            arr[j] = "test:key_" + i;
            arr[j + 1] = "value_" + i;
            if (j == 0)
            {
                jedis.mset(arr);
            }
        }
        long e = System.currentTimeMillis();
        System.out.println("time: " + (e - b));
    }

    /**
     * Test pipeline.
     */
    @Test
    void testPipeline()
    {
        // 创建管道
        Pipeline pipeline = jedis.pipelined();
        long b = System.currentTimeMillis();
        for (int i = 1; i <= 100000; i++)
        {
            // 放入命令到管道
            pipeline.set("test:key_" + i, "value_" + i);
            if (i % 1000 == 0)
            {
                // 每放入1000条命令，批量执行
                pipeline.sync();
            }
        }
        long e = System.currentTimeMillis();
        System.out.println("time: " + (e - b));
    }

    /**
     * Tear down.
     */
    @AfterEach
    void tearDown()
    {
        if (jedis != null)
        {
            jedis.close();
        }
    }
}
