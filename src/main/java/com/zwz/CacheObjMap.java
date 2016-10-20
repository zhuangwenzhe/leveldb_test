package com.zwz;

import org.apache.commons.lang3.StringUtils;
import org.iq80.leveldb.*;
import org.iq80.leveldb.impl.Iq80DBFactory;

import java.io.*;
import java.util.*;

/**
 * Created by zhuangwenzhe on 2016/10/8.
 * 需要的jar levelDB common-lang3
 * <p>
 * <p>
 * 特点：
 * 1、key和value都是任意长度的字节数组；
 * 2、entry（即一条K-V记录）默认是按照key的字典顺序存储的，当然开发者也可以重载这个排序函数；
 * 3、提供的基本操作接口：Put()、Delete()、Get()、Batch()；
 * 4、支持批量操作以原子操作进行；
 * 5、可以创建数据全景的snapshot(快照)，并允许在快照中查找数据；
 * 6、可以通过前向（或后向）迭代器遍历数据（迭代器会隐含的创建一个snapshot）；
 * 7、自动使用Snappy压缩数据；
 * 8、可移植性；
 * 限制：
 * 1、非关系型数据模型（NoSQL），不支持sql语句，也不支持索引；
 * 2、一次只允许一个进程访问一个特定的数据库；
 * 3、没有内置的C/S架构，但开发者可以使用LevelDB库自己封装一个server;
 * 建议：
 * 1、key组成建议使用 业务的id+"_"+时间戳，这样可以类似redis的score那样，采取加减法算出时间范围
 **/

public class CacheObjMap<K, V> extends AbstractMap<K, V> implements Map<K, V>, Cloneable, Serializable {

    // levelDB 的 操作元素
    private DBFactory factory;

    private DB db;

    // 内部的参数
    private File dir; // 操作文件夹的对象 也就是数据存储的路径

    private String path; // 路径 // TODO: 2016/10/8  要设置成可以设置

    public CacheObjMap(String path) {
        this.path = path;
        create();
    }

    public CacheObjMap(File file) {
        this.dir = file;
        create();
    }

    private void create() {
        try {
            this.factory = Iq80DBFactory.factory;
            createDir();
            Options options = new Options().createIfMissing(true);
            this.db = this.factory.open(dir, options);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回个数
     * 模拟hashMap的写法记录个数是错误的，因为这个是可持续化的，leveldb没有提供相关的函数，可能每次查询个数都必须遍历，效率偏低，以前也见过官方类似的写法，但是忘记在哪里见过
     * 如果这个要优化，请考虑同时2个或者2个以上的处理，此时用size标识已经是很不合理
     */
    @Override
    public int size() {
        DBIterator iterator = db.iterator();
        if (!iterator.hasNext()) {
            return 0;
        }
        int i = 0;
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            i++;
        }
        return i;
    }

    /**
     * 是否为空
     * 这个简单，直接递归判断hasNext即可
     *
     * @return
     */
    @Override
    public boolean isEmpty() {
        return !db.iterator().hasNext();
    }

    @Override
    public boolean containsKey(Object key) {
        Iterator<Map.Entry<K, V>> i = entrySet().iterator();
        byte[] keyBytes = null;
        if (key != null) {
            keyBytes = obj2Bytes(key);
        }
        while (i.hasNext()) {
            Map.Entry<K, V> e = i.next();
            if (e.getKey() == keyBytes)
                return true;
        }
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        Iterator<Map.Entry<K, V>> i = entrySet().iterator();
        byte[] valueBytes = null;
        if (value != null) {
            valueBytes = obj2Bytes(value);
        }
        while (i.hasNext()) {
            Map.Entry<K, V> e = i.next();
            if (value == e.getValue())
                return true;
        }
        return false;
    }

    @Override
    public V get(Object key) {
        return (V) bytes2Obj(db.get(obj2Bytes(key)));
    }

    @Override
    public V put(K key, V value) {
        db.put(obj2Bytes(key), obj2Bytes(value));
        return value;
    }

    @Override
    public V remove(Object key) {
        byte[] bytes = obj2Bytes(key);
        V value = bytes2Obj(db.get(bytes));
        db.delete(bytes);
        return value;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        if (m != null && m.size() != 0) {
            WriteBatch batch = db.createWriteBatch();
            for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
                batch.put(obj2Bytes(entry.getKey()), obj2Bytes(entry.getValue()));
            }
            db.write(batch);
        }
    }

    @Override
    public void clear() {
        DBIterator iterator = db.iterator();
        WriteBatch batch = db.createWriteBatch();
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            batch.delete(iterator.peekNext().getKey());
        }
        db.write(batch);
    }

    @Override
    public Set<K> keySet() {
        DBIterator iterator = db.iterator();
        Set<K> set = null;
        if (iterator.hasNext() == true) {
            set = new LinkedHashSet<K>();
        }
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            set.add((K) bytes2Obj(iterator.peekNext().getKey()));
        }
        return set;
    }

    @Override
    public Collection<V> values() {
        DBIterator iterator = db.iterator();
        List<V> list = null;
        if (iterator.hasNext() == true) {
            list = new ArrayList<V>();
        }
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            list.add((V) bytes2Obj(iterator.peekNext().getValue()));
        }
        return list;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        DBIterator iterator = db.iterator();
        Set<Map.Entry<K, V>> set = null;
        if (iterator.hasNext() == true) {
            set = new LinkedHashSet<Map.Entry<K, V>>();
            ;
        }
        for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
            Entry<K, V> entry = new Entry<K, V>((K) bytes2Obj(iterator.peekNext().getKey()), (V) bytes2Obj(iterator.peekNext().getValue()));
            set.add(entry);
        }
        return set;
    }

    private void createDir() throws Exception {
        if (this.dir == null) {
            // 判断路径是否为空
            if (StringUtils.isBlank(this.path)) {
                throw new NullPointerException("路径为空，请设定");
            }
            this.dir = new File(this.path);
            // 文件夹不存在即新建文件夹
            null2Mkdir(this.dir);
            // 如果存在，即不用创建，但要判断指定的路径是否为目录
            if (!this.dir.isDirectory()) {
                throw new Exception("指定的路径非文件夹");
            }
        } else {
            // 文件夹不存在即新建文件夹
            null2Mkdir(this.dir);
            // 如果存在，即不用创建，但要判断指定的路径是否为目录
            if (!this.dir.isDirectory()) {
                throw new Exception("指定的路径非文件夹");
            }
            // 以上步骤都没问题，则设置path
            this.path = dir.getPath();
        }
    }

    public static boolean null2Mkdir(final File file) {
        if (file == null) {
            throw new NullPointerException("参数为空");
        }
        if (file.exists()) {
            return true;
        } else {
            return file.mkdir();
        }
    }

    public static <T> T bytes2Obj(byte[] bytes) {
        if (bytes == null) return null;
        Object obj = null;
        try {
            ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
            ObjectInputStream oi = new ObjectInputStream(bi);
            obj = oi.readObject();
            bi.close();
            oi.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (T) obj;
    }

    public static byte[] obj2Bytes(Object obj) {
        if (obj == null) return null;
        byte[] bytes = null;
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(bo);
            oo.writeObject(obj);
            bytes = bo.toByteArray();
            bo.close();
            oo.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {

        File file = new File("D:\\leveldb_data");
        System.out.println(file.getPath());
        //java.lang.Class<java.util.List<com.zwz.db.User>> clazz = new java.lang.Class<java.util.List<com.zwz.db.User>>() ;
/*        CacheObjMap<Long, List<User>> map = new CacheObjMap<Long,List<User>>();
        List<User> list = new ArrayList<User>();
        User user1 = new User();
        user1.setId(1l);
        user1.setName("zwz");
        User user2 = new User();
        user2.setId(2l);
        user2.setName("zwz2");
        list.add(user1);
        list.add(user2);
        map.put(123l,list);
        for(Map.Entry<Long,List<User>> entry:map.entrySet()){
            System.out.println("key:"+entry.getKey());
            System.out.println("value:"+entry.getValue().get(0).getName());
        }*/
/*        CacheObjMap<Long,User> map = new CacheObjMap<Long,User>
        User user1 = new User();
        user1.setId(1l);
        user1.setName("zwz");
        map.put(1l,user1);
        User user2 = new User();
        user2.setId(2l);
        user2.setName("zwz2");
        map.put(2l,user2);
        for(Map.Entry<Long,User> entry:map.entrySet()){
            System.out.println("key:"+entry.getKey());
            System.out.println("value:"+entry.getValue().getName());

        }*/
    }

    /**
     * 这里的entry和hashmap的不一样，hashmap的主要是存储，而这里只是展示的时候拿来做封装类，和Hashmap不太一样
     *
     * @param <K> key
     * @param <V> value
     */
    static class Entry<K, V> implements Map.Entry<K, V> {

        final K key;

        V value;

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return this.key;
        }

        @Override
        public V getValue() {
            return this.value;
        }

        @Override
        public V setValue(V value) {
            this.value = value;
            return value;
        }
    }
}