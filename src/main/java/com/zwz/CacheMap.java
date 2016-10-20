package com.zwz;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.impl.Iq80DBFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;


/**
 * Created by 庄文哲 on 2016/10/2.
 */
public class CacheMap{

    private final static Logger logger = Logger.getLogger(CacheMap.class);

    private final static String CHARSET_STR ="UTF-8";

    private DBFactory factory;

    private File dir;

    private String path;

    private DB db;

    public static synchronized CacheMap create(){
        return new CacheMap();
    }

    /**
     * 单例模式
     */
    private CacheMap() {
        try {
            this.factory = Iq80DBFactory.factory;
            this.path = "D:\\leveldb_data\\";
            this.dir = new File(this.path);
            db = this.factory.open(dir,null);
        } catch (IOException e) {
            logger.error("创建db工厂失败",e);
        }
    }

    public Long size() {
        Long l = 0l;
        DBIterator iterator = db.iterator();
        while (iterator.hasNext()){
            l++;
        }
        return l;
    }

    public boolean isEmpty() {
        DBIterator iterator = db.iterator();
        return !iterator.hasNext();
    }

    public boolean containsKey(String key) {

    }

    public boolean containsValue(String value) {
        return false;
    }

    public String get(String key) {
        try {
            return new String(db.get(key.getBytes(CHARSET_STR)));
        } catch (UnsupportedEncodingException e) {
            logger.error("编码失败，当前编码只支持:{}",CHARSET_STR,e);
            e.printStackTrace();
            return null;
        } catch (NullPointerException e){
            logger.error("操作的key为空");
            e.printStackTrace();
            return null;
        }
    }

    public boolean put(String key, String value) {
        try {
            db.put(key.getBytes(CHARSET_STR),value.getBytes(CHARSET_STR));
            return true;
        } catch (UnsupportedEncodingException e) {
            logger.error("编码失败，当前编码只支持:{}",CHARSET_STR,e);
            e.printStackTrace();
            return false;
        } catch (NullPointerException e){
            logger.error("操作的key为空");
            e.printStackTrace();
            return false;
        }
    }

    public boolean remove(String key) {
        try {
            db.delete(key.getBytes(CHARSET_STR));
            return true;
        } catch (UnsupportedEncodingException e) {
            logger.error("编码失败，当前编码只支持:{}",CHARSET_STR,e);
            e.printStackTrace();
            return false;
        } catch (NullPointerException e){
            logger.error("操作的key为空");
            e.printStackTrace();
            return false;
        }
    }

    public void putAll(Map<String, String> m) {
        WriteBatch batch = db.createWriteBatch();
        for(Map.Entry<String,String> entry : m.entrySet()){
            batch.put(entry.getKey().getBytes(),entry.getValue().getBytes());
        }
        db.write(batch);
    }

    public void clear() {

    }

    public void destroy() {
        try {
            factory.destroy(this.dir,null);
        } catch (IOException e) {
            logger.error("清空数据库失败",e);
            e.printStackTrace();
        }
    }

    public Set<String> keySet() {
        Set<String> set = new HashSet<String>();
        DBIterator iterator = db.iterator();
        for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()){
            try {
                set.add(new String(iterator.peekNext().getKey(),CHARSET_STR));
            } catch (UnsupportedEncodingException e) {
                logger.error("操作的key为空");
                e.printStackTrace();
            }
        }
        return set;
    }

    public Collection<String> values() {
        List<String> list = new ArrayList<String>();
        DBIterator iterator = db.iterator();
        for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()){
            try {
                list.add(new String(iterator.peekNext().getValue(),CHARSET_STR));
            } catch (UnsupportedEncodingException e) {
                logger.error("操作的key为空");
                e.printStackTrace();
            }
        }
        return list;
    }
}