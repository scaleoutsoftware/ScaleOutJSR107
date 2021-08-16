/*
 Copyright (c) 2018 by ScaleOut Software, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.scaleoutsoftware.soss.cache;

import org.junit.Assert;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.*;
import javax.cache.event.*;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ScaleoutCacheTest {

    Cache<String, String> _cache;
    String _name = "test namespace";

    public ScaleoutCacheTest()
    {
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();

        _cache = manager.createCache(_name, new MutableConfiguration<String, String>().setTypes(String.class, String.class));
    }

    @Test
    public void testCreateCacheAndGetCache() {
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();

        Cache<String, String> cache = manager.createCache(_name, new MutableConfiguration<String, String>().setTypes(String.class, String.class));

        Cache<String, String> cachetwo = manager.getCache(_name, String.class, String.class);
        Assert.assertEquals(cache,cachetwo);
    }

    @Test
    public void get() {
        _cache.put("k1", "v1");
        String ret = _cache.get("k1");
        Assert.assertEquals("v1", ret);

        _cache.remove("k1");
        Assert.assertNull(_cache.get("k1"));
    }

    @Test
    public void getWithLargeKey() {
        String key =    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
                        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" +
                        "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC" +
                        "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD";
        _cache.put(key, "v1");
        String ret = _cache.get(key);
        Assert.assertEquals("v1", ret);

        _cache.remove("k1");
        Assert.assertNull(_cache.get("k1"));
    }

    @Test
    public void getWithLargeCustomClassKey() {
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        Cache<CustomKeyType, String> cache = manager.getCache("customKeyTest", CustomKeyType.class, String.class);
        String keyValue = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
                "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" +
                "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC" +
                "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD";
        CustomKeyType key = new CustomKeyType(keyValue);
        cache.put(key, "v1");
        String ret = cache.get(key);
        Assert.assertEquals("v1", ret);

        cache.remove(key);
        Assert.assertNull(cache.get(key));
    }

    @Test
    public void testExpiredWithLargeCustomClassKey() {
        long timeout = 1;
        long currentTimeMillis = System.currentTimeMillis() + (1000 * timeout);
        int roundToNearestSecond = (int)((currentTimeMillis + 500) / 1000);
        CacheEntryListenerConfiguration<CustomKeyType, String> configuration = getCacheEntryListenerConfigurationCustomKey(roundToNearestSecond);
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        Cache<CustomKeyType, String> cache = manager.createCache(_name+"expiredCustomKey", new MutableConfiguration<CustomKeyType,String>()
            .setTypes(CustomKeyType.class, String.class)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, timeout)))
            .addCacheEntryListenerConfiguration(configuration));
        String keyValue = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
                "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" +
                "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC" +
                "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD";
        CustomKeyType key = new CustomKeyType(keyValue);
        cache.put(key, "bar");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Assert.fail();
        }
        cache.deregisterCacheEntryListener(configuration);
    }


    @Test
    public void getAllremoveAll() {
        _cache.clear();
        _cache.put("k1", "v1");
        _cache.put("k2", "v2");

        Set<String> keys = new HashSet<>();
        keys.add("k1");
        keys.add("k2");
        keys.add("k3"); // won't be found

        Map<String, String> res = _cache.getAll(keys);
        Assert.assertEquals(2, res.size());
        Assert.assertEquals("v1", res.get("k1"));
        Assert.assertEquals("v2", res.get("k2"));
        Assert.assertNull(res.get("k3"));

        _cache.removeAll(keys);
        Assert.assertNull(_cache.get("k1"));
        Assert.assertNull(_cache.get("k2"));
    }

    @Test
    public void containsKey() {
        _cache.put("k1", "v1");
        Assert.assertTrue(_cache.containsKey("k1"));

        _cache.remove("k1");
        Assert.assertFalse(_cache.containsKey("k1"));
    }

    @Test
    public void getAndPut() {
        String result = _cache.getAndPut("k1", "v1");
        Assert.assertNull(result);

        result = _cache.getAndPut("k1", "v2");
        Assert.assertEquals("v1", result);

        result = _cache.get("k1");
        Assert.assertEquals("v2", result);

        _cache.remove("k1");
        Assert.assertNull(_cache.get("k1"));
    }

    @Test
    public void putAll() {
        Map<String, String> kvMap = new HashMap<>();
        kvMap.put("k1", "v1");
        kvMap.put("k2", "v2");

        _cache.putAll(kvMap);

        Map<String, String> res = _cache.getAll(kvMap.keySet());
        Assert.assertEquals(2, res.size());
        Assert.assertEquals("v1", res.get("k1"));
        Assert.assertEquals("v2", res.get("k2"));

        _cache.removeAll(kvMap.keySet());
        Assert.assertNull(_cache.get("k1"));
        Assert.assertNull(_cache.get("k2"));
    }

    @Test
    public void putIfAbsent() {
        boolean wasPut = _cache.putIfAbsent("k1", "v1");
        Assert.assertTrue(wasPut);

        wasPut = _cache.putIfAbsent("k1", "will fail");
        Assert.assertFalse(wasPut);

        _cache.remove("k1");
        Assert.assertNull(_cache.get("k1"));
    }

    @Test
    public void removeIfValMatches() {
        boolean wasRemoved = _cache.remove("k666", "will fail");

        _cache.put("k1", "v1");

        wasRemoved = _cache.remove("k1", "v99");
        Assert.assertFalse(wasRemoved);

        wasRemoved = _cache.remove("k1", "v1");
        Assert.assertTrue(wasRemoved);
    }


    @Test
    public void getAndRemove() {

        String ret = _cache.getAndRemove("k43243");
        Assert.assertNull(ret);

        _cache.put("k1", "v1");

        ret = _cache.getAndRemove("k1");
        Assert.assertEquals("v1", ret);
        Assert.assertNull(_cache.get("k1"));
    }

    @Test
    public void replace() {
        boolean wasReplaced = _cache.replace("k392", "will fail");
        Assert.assertFalse(wasReplaced);

        _cache.put("k1", "v1");
        wasReplaced = _cache.replace("k1", "v2");
        Assert.assertTrue(wasReplaced);
        Assert.assertEquals("v2", _cache.get("k1"));

        _cache.remove("k1");
    }

    @Test
    public void replaceIfValMatches() {
        boolean wasReplaced = _cache.replace("k994", "v3838", "will fail");
        Assert.assertFalse(wasReplaced);

        _cache.put("k1", "v1");
        wasReplaced = _cache.replace("k1", "bad old value", "will fail");
        Assert.assertFalse(wasReplaced);

        wasReplaced = _cache.replace("k1", "v1", "v2");
        Assert.assertTrue(wasReplaced);
        Assert.assertEquals("v2", _cache.get("k1"));

        _cache.remove("k1");
    }

    @Test
    public void getAndReplace() {
        String ret = _cache.getAndReplace("k23890", "will fail");
        Assert.assertNull(ret);
        Assert.assertNull(_cache.get("k23890"));

        _cache.put("k1", "v1");
        ret = _cache.getAndReplace("k1", "v2");
        Assert.assertEquals("v1", ret);
        Assert.assertEquals("v2", _cache.get("k1"));

        _cache.remove("k1");
    }

    @Test
    public void clear() {
        _cache.put("k1", "v1");
        _cache.clear();

        // can't remember if this is async or not. Sleep for a sec to be safe.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertNull(_cache.get("k1"));
    }

    @Test
    public void getName() {
        Assert.assertEquals(_name, _cache.getName());
    }

    @Test
    public void testExpired() {
        long timeout = 1;
        CachingProvider provider = new ScaleoutCachingProvider();
        Properties props = provider.getDefaultProperties();

        props.setProperty("object_expiration_timeout_secs", Long.toString(timeout));
        props.setProperty("object_expiration_timeout_policy", "absolute");
        CacheManager manager = provider.getCacheManager(provider.getDefaultURI(), provider.getDefaultClassLoader(), props);
        Cache<String, String> cache = manager.getCache(_name + "_expired", String.class, String.class);

        long currentTimeMillis = System.currentTimeMillis() + (1000 * timeout);
        int roundToNearestSecond = (int)((currentTimeMillis + 500) / 1000);

        CacheEntryListenerConfiguration<String, String> configuration = getCacheEntryListenerConfiguration(roundToNearestSecond);
        cache.registerCacheEntryListener(configuration);
        cache.put("foo", "bar");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cache.deregisterCacheEntryListener(configuration);
    }

    @Test
    public void testExpiredThroughConfig() {
        long timeout = 1;
        long currentTimeMillis = System.currentTimeMillis() + (1000 * timeout);
        int roundToNearestSecond = (int)((currentTimeMillis + 500) / 1000);
        CacheEntryListenerConfiguration<String, String> configuration = getCacheEntryListenerConfiguration(roundToNearestSecond);
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        Cache<String,String> cache = manager.createCache(_name+"_expiredThroughConfig", new MutableConfiguration<String,String>()
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, timeout)))
            .addCacheEntryListenerConfiguration(configuration));

        cache.put("foo", "bar");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cache.deregisterCacheEntryListener(configuration);
    }

    @Test
    public void testCorrectExpiredPolicyThroughConfig() {
        long timeout = 1;
        // round to nearest second * 2 because we will touch the object
        long currentTimeMillis = System.currentTimeMillis() + ((1000 * timeout) * 2);
        int roundToNearestSecond = (int)((currentTimeMillis + 500) / 1000);
        CacheEntryListenerConfiguration<String, String> configuration = getCacheEntryListenerConfiguration(roundToNearestSecond);
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        Cache<String,String> cache = manager.createCache(_name+"_expiredThroughConfigSliding", new MutableConfiguration<String,String>()
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 1)))
                .addCacheEntryListenerConfiguration(configuration));
        cache.put("foo", "bar");
        try {
            Thread.sleep(750);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail();
        }
        cache.get("foo");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail();
        }
        cache.deregisterCacheEntryListener(configuration);
    }

    @Test
    public void testRemoveCacheEntryListenerConfiguration() {
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        CacheEntryListenerConfiguration<String, String> configuration = getCacheEntryListenerConfiguration(1);
        _cache = manager.createCache(_name, new MutableConfiguration<String,String>()
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 1)))
                .addCacheEntryListenerConfiguration(configuration));
        _cache.put("foo", "bar");
        _cache.deregisterCacheEntryListener(configuration);
    }

    @Test
    public void testClose() {
        Exception ex = null;
        _cache.close();
        try {
            _cache.put("k", "v");
        } catch(Exception e) {
            ex = e;
        }
        Assert.assertTrue(ex instanceof IllegalStateException);
        ex = null;

        try {
            _cache.getAll(new HashSet<String>());
        } catch(Exception e) {
            ex = e;
        }
        Assert.assertTrue(ex instanceof IllegalStateException);
        ex = null;

        try {
            _cache.get("l");
        } catch(Exception e) {
            ex = e;
        }
        Assert.assertTrue(ex instanceof IllegalStateException);
        ex = null;

        try {
            _cache.clear();
        } catch(Exception e) {
            ex = e;
        }
        Assert.assertTrue(ex instanceof IllegalStateException);
        ex = null;
    }

    @Test
    public void testInvoke() {
        _cache.put("k1", "v1");
        String ret = _cache.invoke("k1", new EntryProcessor<String, String, String>() {
            @Override
            public String process(MutableEntry<String, String> mutableEntry, Object... objects) throws EntryProcessorException {
                return "foo";
            }
        });


        Assert.assertEquals("foo", ret);
    }
    @Test
    public void testInvokeAll() {
        Set<String> keys = new HashSet<String>();
        keys.add("k1");
        keys.add("k2");
        keys.add("k3"); // won't be found
        _cache.put("k1", "v1");
        _cache.put("k2", "v2");

        Map<String, EntryProcessorResult<String>> ret = _cache.invokeAll(keys, new EntryProcessor<String, String, String>() {
            @Override
            public String process(MutableEntry<String, String> mutableEntry, Object... objects) throws EntryProcessorException {
                Assert.assertNotNull(mutableEntry.getKey());
                if(mutableEntry.getKey().compareTo("k3") == 0) {
                    Assert.assertNull(mutableEntry.getValue());
                }
                return "foo";
            }
        });
        for(Map.Entry<String, EntryProcessorResult<String>> entry : ret.entrySet()) {
            Assert.assertEquals(0, entry.getValue().get().compareTo("foo"));
        }
    }

    @Test
    public void testReadThrough() {
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setReadThrough(true);
        config.setCacheLoaderFactory(new Factory<CacheLoader<String, String>>() {
            @Override
            public CacheLoader<String, String> create() {
                return new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws CacheLoaderException {
                        File f = new File(key);
                        Assert.assertTrue(f.exists());
                        try {
                            FileInputStream fis = new FileInputStream(f);
                            byte[] data = new byte[(int) f.length()];
                            fis.read(data);
                            fis.close();

                            return new String(data, StandardCharsets.UTF_8);
                        } catch (FileNotFoundException e) {
                            Assert.fail();
                            return null;
                        } catch (UnsupportedEncodingException e) {
                            Assert.fail();
                            return null;
                        } catch (IOException e) {
                            Assert.fail();
                            return null;
                        }
                    }

                    @Override
                    public Map<String, String> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
                        return null;
                    }
                };
            }
        });
        Cache<String, String> cache = manager.createCache("TestReadThrough", config);

        String expected = "HelloReadThrough.";
        String actual = cache.get("test_readthrough.txt");
        Assert.assertEquals(expected, actual);

    }

    @Test
    public void testWriteThroughWrite() {
        File f = new File("test_writethrough.txt");
        f.delete();
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setWriteThrough(true);
        config.setCacheWriterFactory(new Factory<CacheWriter<? super String, ? super String>>() {
            @Override
            public CacheWriter<? super String, ? super String> create() {
                return new CacheWriter<String, String>() {
                    @Override
                    public void write(Cache.Entry<? extends String, ? extends String> entry) throws CacheWriterException {
                        try {
                            File f = new File(entry.getKey());
                            f.createNewFile();
                            FileOutputStream stream = new FileOutputStream(f);
                            stream.write(entry.getValue().getBytes());
                            stream.flush();
                            stream.close();
                        } catch (FileNotFoundException e) {
                            Assert.fail();
                        } catch (IOException e) {
                            Assert.fail();
                        }
                    }

                    @Override
                    public void writeAll(Collection<Cache.Entry<? extends String, ? extends String>> collection) throws CacheWriterException {

                    }

                    @Override
                    public void delete(Object o) throws CacheWriterException {

                    }

                    @Override
                    public void deleteAll(Collection<?> collection) throws CacheWriterException {

                    }
                };
            }
        });
        Cache<String, String> cache = manager.createCache("TestWriteThrough", config);

        String expected = "HelloWriteThrough.";
        cache.put("test_writethrough.txt", expected);

        Assert.assertTrue(f.exists());
        try {
            FileInputStream fis = new FileInputStream(f);
            byte[] data = new byte[(int) f.length()];
            fis.read(data);
            fis.close();

           Assert.assertEquals(expected, new String(data, StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            Assert.fail();
        } catch (UnsupportedEncodingException e) {
            Assert.fail();
        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void testWriteThroughDelete() {
        File f = new File("test_writethrough_delete.txt");
        f.delete();
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setWriteThrough(true);
        config.setCacheWriterFactory(new Factory<CacheWriter<? super String, ? super String>>() {
            @Override
            public CacheWriter<? super String, ? super String> create() {
                return new CacheWriter<String, String>() {
                    @Override
                    public void write(Cache.Entry<? extends String, ? extends String> entry) throws CacheWriterException {
                        try {
                            File f = new File(entry.getKey());
                            f.createNewFile();
                            FileOutputStream stream = new FileOutputStream(f);
                            stream.write(entry.getValue().getBytes());
                            stream.flush();
                            stream.close();
                        } catch (FileNotFoundException e) {
                            Assert.fail();
                        } catch (IOException e) {
                            Assert.fail();
                        }
                    }

                    @Override
                    public void writeAll(Collection<Cache.Entry<? extends String, ? extends String>> collection) throws CacheWriterException {

                    }

                    @Override
                    public void delete(Object o) throws CacheWriterException {
                        try {
                            File f = new File(o.toString());
                            f.createNewFile();
                            FileOutputStream stream = new FileOutputStream(f);
                            stream.write("Delete.".getBytes(StandardCharsets.UTF_8));
                            stream.flush();
                            stream.close();
                        } catch (FileNotFoundException e) {
                            Assert.fail();
                        } catch (IOException e) {
                            Assert.fail();
                        }
                    }

                    @Override
                    public void deleteAll(Collection<?> collection) throws CacheWriterException {

                    }
                };
            }
        });
        Cache<String, String> cache = manager.createCache("TestWriteThrough", config);

        String expected = "Delete.";
        cache.put("test_writethrough_delete.txt", expected);
        cache.remove("test_writethrough_delete.txt");
        Assert.assertTrue(f.exists());
        try {
            FileInputStream fis = new FileInputStream(f);
            byte[] data = new byte[(int) f.length()];
            fis.read(data);
            fis.close();

            Assert.assertEquals(expected, new String(data, StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            Assert.fail();
        } catch (UnsupportedEncodingException e) {
            Assert.fail();
        } catch (IOException e) {
            Assert.fail();
        }
    }


    private CacheEntryListenerConfiguration<String, String> getCacheEntryListenerConfiguration(final long expected) {
        return new CacheEntryListenerConfiguration<String, String>() {
            @Override
            public Factory<CacheEntryListener<? super String, ? super String>> getCacheEntryListenerFactory() {
                return new Factory<CacheEntryListener<? super String, ? super String>>() {
                    @Override
                    public CacheEntryListener<? super String, ? super String> create() {
                        return new CacheEntryExpiredListener<String, String>() {
                            @Override
                            public void onExpired(Iterable<CacheEntryEvent<? extends String, ? extends String>> iterable) throws CacheEntryListenerException {
                                long currentTimeMillis = System.currentTimeMillis();
                                int roundToNearestSecond = (int)((currentTimeMillis + 500) / 1000);
                                System.out.println("Actual: " + roundToNearestSecond + " expected: " + expected);
                                for(CacheEntryEvent<? extends String, ? extends String> kvp : iterable) {
                                    Assert.assertEquals(roundToNearestSecond, expected);
                                    Assert.assertEquals(kvp.getValue(), "bar");
                                }
                            }
                        };
                    }
                };
            }

            @Override
            public boolean isOldValueRequired() {
                return false;
            }

            @Override
            public Factory<CacheEntryEventFilter<? super String, ? super String>> getCacheEntryEventFilterFactory() {
                return null;
            }

            @Override
            public boolean isSynchronous() {
                return false;
            }
        };
    }

    private CacheEntryListenerConfiguration<CustomKeyType, String> getCacheEntryListenerConfigurationCustomKey(final long expected) {
        return new CacheEntryListenerConfiguration<CustomKeyType, String>() {
            @Override
            public Factory<CacheEntryListener<? super CustomKeyType, ? super String>> getCacheEntryListenerFactory() {
                return new Factory<CacheEntryListener<? super CustomKeyType, ? super String>>() {
                    @Override
                    public CacheEntryListener<? super CustomKeyType, ? super String> create() {
                        return new CacheEntryExpiredListener<CustomKeyType, String>() {
                            @Override
                            public void onExpired(Iterable<CacheEntryEvent<? extends CustomKeyType, ? extends String>> iterable) throws CacheEntryListenerException {
                                long currentTimeMillis = System.currentTimeMillis();
                                int roundToNearestSecond = (int)((currentTimeMillis + 500) / 1000);
                                System.out.println("Actual: " + roundToNearestSecond + " expected: " + expected);
                                for(CacheEntryEvent<? extends CustomKeyType, ? extends String> kvp : iterable) {
                                    Assert.assertEquals(roundToNearestSecond, expected);
                                    Assert.assertEquals(kvp.getValue(), "bar");
                                }
                            }
                        };
                    }
                };
            }

            @Override
            public boolean isOldValueRequired() {
                return false;
            }

            @Override
            public Factory<CacheEntryEventFilter<? super CustomKeyType, ? super String>> getCacheEntryEventFilterFactory() {
                return null;
            }

            @Override
            public boolean isSynchronous() {
                return false;
            }
        };
    }

    private static class CustomKeyType implements Serializable {
        String _keyValue;
        public CustomKeyType(String value) {
            _keyValue = value;
        }

        @Override
        public String toString() {
            return "CustomKeyType{" +
                    "_keyValue='" + _keyValue + '\'' +
                    '}';
        }
    }
}