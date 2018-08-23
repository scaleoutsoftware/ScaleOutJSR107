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
import javax.cache.event.*;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ScaleoutCacheTest {

    Cache<String, String> _cache;
    String _name = "test namespace";

    public ScaleoutCacheTest()
    {
        CachingProvider provider = new ScaleoutCachingProvider();
        CacheManager manager = provider.getCacheManager();
        _cache = manager.getCache(_name, String.class, String.class);
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
        CachingProvider provider = new ScaleoutCachingProvider();
        Properties props = provider.getDefaultProperties();
        props.setProperty("object_expiration_timeout_secs", "1");
        props.setProperty("object_expiration_timeout_policy", "absolute");
        CacheManager manager = provider.getCacheManager(provider.getDefaultURI(), provider.getDefaultClassLoader(), props);
        _cache = manager.getCache(_name, String.class, String.class);
        _cache.put("foo", "bar");
        String isnullstring = null;
        CacheEntryListenerConfiguration<String, String> config = new CacheEntryListenerConfiguration<String, String>() {
            @Override
            public Factory<CacheEntryListener<? super String, ? super String>> getCacheEntryListenerFactory() {
                return new Factory<CacheEntryListener<? super String, ? super String>>() {
                    @Override
                    public CacheEntryListener<? super String, ? super String> create() {
                        return new CacheEntryExpiredListener<String, String>() {
                            @Override
                            public void onExpired(Iterable<CacheEntryEvent<? extends String, ? extends String>> iterable) throws CacheEntryListenerException {
                                for(CacheEntryEvent<? extends String, ? extends String> kvp : iterable) {
                                    System.out.println("key: " + kvp.getKey() + " value: " + kvp.getValue());
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
        _cache.registerCacheEntryListener(config);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
                System.out.println(mutableEntry.getValue());
                return "foo";
            }
        });


        assertEquals(0, ret.compareTo("foo"));
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
}