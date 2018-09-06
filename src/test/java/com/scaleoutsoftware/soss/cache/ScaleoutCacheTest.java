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

import com.scaleoutsoftware.soss.cache.ScaleoutCachingProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.*;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ScaleoutCacheTest {

    Cache<String, String> _cache;
    String _name = "test namespace";

    public ScaleoutCacheTest()
    {
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();

        _cache = manager.createCache(_name, new MutableConfiguration<String, String>().setTypes(String.class, String.class));
    }

    @org.junit.jupiter.api.Test
    void testCreateCacheAndGetCache() {
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();

        Cache<String, String> cache = manager.createCache(_name, new MutableConfiguration<String, String>().setTypes(String.class, String.class));

        Cache<String, String> cachetwo = manager.getCache(_name, String.class, String.class);
        assertEquals(cache,cachetwo);
    }

    @org.junit.jupiter.api.Test
    void get() {
        _cache.put("k1", "v1");
        String ret = _cache.get("k1");
        Assertions.assertEquals("v1", ret);

        _cache.remove("k1");
        Assertions.assertNull(_cache.get("k1"));
    }

    @org.junit.jupiter.api.Test
    void getWithLargeKey() {
        String key =    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
                        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" +
                        "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC" +
                        "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD";
        _cache.put(key, "v1");
        String ret = _cache.get(key);
        Assertions.assertEquals("v1", ret);

        _cache.remove("k1");
        Assertions.assertNull(_cache.get("k1"));
    }

    @org.junit.jupiter.api.Test
    void getWithLargeCustomClassKey() {
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
        Assertions.assertEquals("v1", ret);

        cache.remove(key);
        Assertions.assertNull(cache.get(key));
    }

    @org.junit.jupiter.api.Test
    void testExpiredWithLargeCustomClassKey() {
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
            fail();
        }
        cache.deregisterCacheEntryListener(configuration);
    }


    @org.junit.jupiter.api.Test
    void getAllremoveAll() {
        _cache.clear();
        _cache.put("k1", "v1");
        _cache.put("k2", "v2");

        Set<String> keys = new HashSet<>();
        keys.add("k1");
        keys.add("k2");
        keys.add("k3"); // won't be found

        Map<String, String> res = _cache.getAll(keys);
        Assertions.assertEquals(2, res.size());
        Assertions.assertEquals("v1", res.get("k1"));
        Assertions.assertEquals("v2", res.get("k2"));
        Assertions.assertNull(res.get("k3"));

        _cache.removeAll(keys);
        Assertions.assertNull(_cache.get("k1"));
        Assertions.assertNull(_cache.get("k2"));
    }

    @org.junit.jupiter.api.Test
    void containsKey() {
        _cache.put("k1", "v1");
        Assertions.assertTrue(_cache.containsKey("k1"));

        _cache.remove("k1");
        Assertions.assertFalse(_cache.containsKey("k1"));
    }

    @org.junit.jupiter.api.Test
    void getAndPut() {
        String result = _cache.getAndPut("k1", "v1");
        Assertions.assertNull(result);

        result = _cache.getAndPut("k1", "v2");
        Assertions.assertEquals("v1", result);

        result = _cache.get("k1");
        Assertions.assertEquals("v2", result);

        _cache.remove("k1");
        Assertions.assertNull(_cache.get("k1"));
    }

    @org.junit.jupiter.api.Test
    void putAll() {
        Map<String, String> kvMap = new HashMap<>();
        kvMap.put("k1", "v1");
        kvMap.put("k2", "v2");

        _cache.putAll(kvMap);

        Map<String, String> res = _cache.getAll(kvMap.keySet());
        Assertions.assertEquals(2, res.size());
        Assertions.assertEquals("v1", res.get("k1"));
        Assertions.assertEquals("v2", res.get("k2"));

        _cache.removeAll(kvMap.keySet());
        Assertions.assertNull(_cache.get("k1"));
        Assertions.assertNull(_cache.get("k2"));
    }

    @org.junit.jupiter.api.Test
    void putIfAbsent() {
        boolean wasPut = _cache.putIfAbsent("k1", "v1");
        Assertions.assertTrue(wasPut);

        wasPut = _cache.putIfAbsent("k1", "will fail");
        Assertions.assertFalse(wasPut);

        _cache.remove("k1");
        Assertions.assertNull(_cache.get("k1"));
    }

    @org.junit.jupiter.api.Test
    void removeIfValMatches() {
        boolean wasRemoved = _cache.remove("k666", "will fail");

        _cache.put("k1", "v1");

        wasRemoved = _cache.remove("k1", "v99");
        Assertions.assertFalse(wasRemoved);

        wasRemoved = _cache.remove("k1", "v1");
        Assertions.assertTrue(wasRemoved);
    }


    @org.junit.jupiter.api.Test
    void getAndRemove() {

        String ret = _cache.getAndRemove("k43243");
        Assertions.assertNull(ret);

        _cache.put("k1", "v1");

        ret = _cache.getAndRemove("k1");
        Assertions.assertEquals("v1", ret);
        Assertions.assertNull(_cache.get("k1"));
    }

    @org.junit.jupiter.api.Test
    void replace() {
        boolean wasReplaced = _cache.replace("k392", "will fail");
        Assertions.assertFalse(wasReplaced);

        _cache.put("k1", "v1");
        wasReplaced = _cache.replace("k1", "v2");
        Assertions.assertTrue(wasReplaced);
        Assertions.assertEquals("v2", _cache.get("k1"));

        _cache.remove("k1");
    }

    @org.junit.jupiter.api.Test
    void replaceIfValMatches() {
        boolean wasReplaced = _cache.replace("k994", "v3838", "will fail");
        Assertions.assertFalse(wasReplaced);

        _cache.put("k1", "v1");
        wasReplaced = _cache.replace("k1", "bad old value", "will fail");
        Assertions.assertFalse(wasReplaced);

        wasReplaced = _cache.replace("k1", "v1", "v2");
        Assertions.assertTrue(wasReplaced);
        Assertions.assertEquals("v2", _cache.get("k1"));

        _cache.remove("k1");
    }

    @org.junit.jupiter.api.Test
    void getAndReplace() {
        String ret = _cache.getAndReplace("k23890", "will fail");
        Assertions.assertNull(ret);
        Assertions.assertNull(_cache.get("k23890"));

        _cache.put("k1", "v1");
        ret = _cache.getAndReplace("k1", "v2");
        Assertions.assertEquals("v1", ret);
        Assertions.assertEquals("v2", _cache.get("k1"));

        _cache.remove("k1");
    }

    @org.junit.jupiter.api.Test
    void clear() {
        _cache.put("k1", "v1");
        _cache.clear();

        // can't remember if this is async or not. Sleep for a sec to be safe.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assertions.assertNull(_cache.get("k1"));
    }

    @org.junit.jupiter.api.Test
    void getName() {
        Assertions.assertEquals(_name, _cache.getName());
    }

    @org.junit.jupiter.api.Test
    void  testExpired() {
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

    @org.junit.jupiter.api.Test
    void  testExpiredThroughConfig() {
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

    @org.junit.jupiter.api.Test
    void  testCorrectExpiredPolicyThroughConfig() {
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
            fail();
        }
        cache.get("foo");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        cache.deregisterCacheEntryListener(configuration);
    }

    @org.junit.jupiter.api.Test
    void  testRemoveCacheEntryListenerConfiguration() {
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        CacheEntryListenerConfiguration<String, String> configuration = getCacheEntryListenerConfiguration(1);
        _cache = manager.createCache(_name, new MutableConfiguration<String,String>()
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 1)))
                .addCacheEntryListenerConfiguration(configuration));
        _cache.put("foo", "bar");
        _cache.deregisterCacheEntryListener(configuration);
    }

    @org.junit.jupiter.api.Test
    void testClose() {
        Exception ex = null;
        _cache.close();
        try {
            _cache.put("k", "v");
        } catch(Exception e) {
            ex = e;
        }
        assertTrue(ex instanceof IllegalStateException);
        ex = null;

        try {
            _cache.getAll(new HashSet<String>());
        } catch(Exception e) {
            ex = e;
        }
        assertTrue(ex instanceof IllegalStateException);
        ex = null;

        try {
            _cache.get("l");
        } catch(Exception e) {
            ex = e;
        }
        assertTrue(ex instanceof IllegalStateException);
        ex = null;

        try {
            _cache.clear();
        } catch(Exception e) {
            ex = e;
        }
        assertTrue(ex instanceof IllegalStateException);
        ex = null;
    }

    @org.junit.jupiter.api.Test
    void testInvoke() {
        _cache.put("k1", "v1");
        String ret = _cache.invoke("k1", new EntryProcessor<String, String, String>() {
            @Override
            public String process(MutableEntry<String, String> mutableEntry, Object... objects) throws EntryProcessorException {
                return "foo";
            }
        });


        assertEquals("foo", ret);
    }
    @org.junit.jupiter.api.Test
    void testInvokeAll() {
        Set<String> keys = new HashSet<String>();
        keys.add("k1");
        keys.add("k2");
        keys.add("k3"); // won't be found
        _cache.put("k1", "v1");
        _cache.put("k2", "v2");

        Map<String, EntryProcessorResult<String>> ret = _cache.invokeAll(keys, new EntryProcessor<String, String, String>() {
            @Override
            public String process(MutableEntry<String, String> mutableEntry, Object... objects) throws EntryProcessorException {
                assertNotNull(mutableEntry.getKey());
                if(mutableEntry.getKey().compareTo("k3") == 0) {
                    assertNull(mutableEntry.getValue());
                }
                return "foo";
            }
        });
        for(Map.Entry<String, EntryProcessorResult<String>> entry : ret.entrySet()) {
            assertEquals(0, entry.getValue().get().compareTo("foo"));
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
                                    assertEquals(roundToNearestSecond, expected);
                                    assertEquals(kvp.getValue(), "bar");
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
                                    assertEquals(roundToNearestSecond, expected);
                                    assertEquals(kvp.getValue(), "bar");
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