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

import com.scaleoutsoftware.soss.client.*;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.EventType;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * <p>
 *     The ScaleoutCache is an implementation of the JSR107 {@link Cache} interface. This implementation stores key/value pairs
 *     inside ScaleOut StateServer.
 * </p>
 *
 * <p>
 *    The namespace used by SOSS is configured through the {@link ScaleoutCacheManager#getCache(String)} method.
 * </p>
 *
 * <p>
 *     By default, the ScaleOutCache stores {@link Serializable} keys and values. You can control the serialization
 *     strategy by setting properties on the {@link Properties} provided by the CachingProvider. i.e.,
 * </p>
 *
 * <pre>{@code
 *     CachingProvider provider = new ScaleoutCachingProvider();
 *     Properties props = provider.getDefaultProperties();
 *     props.setProperty("...", "...");
 *     props.setProperty("...", "...");
 *     CacheManager manager = provider.getCacheManager(provider.getDefaultURI(), provider.getDefaultClassLoader(), props);
 * }</pre>
 *
 * <p>
 *     property name:
 *     cache_key_serializer
 *     default value:
 *     none
 * </P>
 *
 * <p>
 *     property name:
 *     cache_value_serializer
 *     default value:
 *     none
 * </p>
 *
 * <p>
 *     property name:
 *     object_expiration_timeout_secs
 *     default value:
 *     none
 * </p>
 *
 * <p>
 *     property name:
 *     object_expiration_timeout_policy
 *     default value:
 *     absolute
 * </p>
 *
 * <p>
 *     All {@link ScaleoutCache} operations can throw a CacheException and the underlying exception may be specific to ScaleOut StateServer (SOSS).
 *     The Javadoc for the SOSS Java APIs describes the underlying exceptions in detail. For example,
 *     SOSS configuration exceptions are specific to SOSS and thrown as a CacheException.
 * </p>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class ScaleoutCache<K,V> implements Cache<K, V> {

    // this should be put in a isolated parallel class, i.e. Parallel.getAll(set< ? extends K>);
    private final ExecutorService   _service    = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final Logger            _logger     = Logger.getLogger(ScaleoutCache.class.getName());
    private final HashMap<CacheEntryListenerConfiguration, NamedCacheObjectExpirationListener>
                                    _listeners  = new HashMap<>();
    private boolean                 _closed     = false;

    private final NamedCache                        _cache;
    private final ScaleoutCacheConfiguration<K,V>   _config;
    private final ScaleoutCacheManager              _manager;
    private final Class<K>                          _keyType;
    private final Class<V>                          _valueType;
    private CustomSerializer<K>                     _keySerializer;



    /**
     * <p>
     *     Instantiates a ScaleOutCache with the parameter {@link ScaleoutCacheConfiguration} and {@link ScaleoutCacheManager}.
     * </p>
     *
     * @param config the configuration to use for this cache
     * @param manager the manager responsible for this cache
     */
    ScaleoutCache(ScaleoutCacheConfiguration<K,V> config, ScaleoutCacheManager manager) {
        _logger.info("Initializing ScaleOut Cache version 1.2.");
        _logger.info("Configuration supplied.");
        _logger.info(config.toString());
        // handle key/value types
        _config     = config;
        _manager    = manager;
        _keyType    = _config.getKeyType();
        _valueType  = _config.getValueType();

        // retrieve NamedCache
        try {
            _cache = CacheFactory.getCache(config.getCacheName());
        } catch (NamedCacheException e) {
            throw new IllegalArgumentException("Could not create cache.");
        }

        // handle key/value serializers
        String valueClassName = _manager.getProperties().getProperty("cache_value_serializer");
        if(valueClassName != null) {
            try {
                Class<? extends CustomSerializer> valueSerializerClazz = (Class<? extends CustomSerializer>)ClassLoader.getSystemClassLoader().loadClass(valueClassName);
                CustomSerializer serializer = valueSerializerClazz.newInstance();
                _cache.setCustomSerialization(serializer);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not create custom value serializer: " + valueClassName, e);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException("Could not access custom value serializer: " + valueClassName, iae);
            } catch (InstantiationException ie) {
                throw new IllegalArgumentException("Could not instantiate custom value serializer: " + valueClassName, ie);
            }
        }

        String keyClassName = _manager.getProperties().getProperty("cache_key_serializer");
        if(valueClassName != null) {
            try {
                Class<? extends CustomSerializer> keySerializerClazz = (Class<? extends CustomSerializer>)ClassLoader.getSystemClassLoader().loadClass(keyClassName);
                _keySerializer = keySerializerClazz.newInstance();
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not create custom key serializer: " + valueClassName, e);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException("Could not access custom key serializer: " + valueClassName, iae);
            } catch (InstantiationException ie) {
                throw new IllegalArgumentException("Could not instantiate custom key serializer: " + valueClassName, ie);
            }
        }

        // handle create/access/update timeouts through properties
        String timeoutType              = "none";
        CreatePolicy cacheCreatePolicy  = _cache.getDefaultCreatePolicy();

        String objectExpirationTimeout          = _manager.getProperties().getProperty("object_expiration_timeout_secs");
        String objectExpirationTimeoutPolicy    = _manager.getProperties().getProperty("object_expiration_timeout_policy", "absolute");
        if(objectExpirationTimeout != null) {
            int timeout;
            try {
                timeout = Integer.parseInt(objectExpirationTimeout);
            } catch (Exception e) {
                throw new CacheException("Could not convert timeout: " + objectExpirationTimeout);
            }

            if(objectExpirationTimeoutPolicy != null) {
                timeoutType = objectExpirationTimeoutPolicy;
                switch(objectExpirationTimeoutPolicy) {
                    case "absolute":
                        cacheCreatePolicy.setAbsoluteTimeout(true);
                        break;
                    case "sliding":
                        cacheCreatePolicy.setAbsoluteTimeout(false);
                        break;
                    case "absolute_on_read":
                        cacheCreatePolicy.setAbsoluteTimeoutOnRead(true);
                        break;
                    default:
                        throw new CacheException("Unknown object expiration timeout policy: " + objectExpirationTimeoutPolicy);
                }
            }
            cacheCreatePolicy.setTimeout(TimeSpan.fromSeconds(timeout));
        }

        // handle create/access/update timeouts through config
        Factory<ExpiryPolicy> expiryPolicyFactory = config.getExpiryPolicyFactory();
        if(expiryPolicyFactory != null && expiryPolicyFactory.create() != null) {
            ExpiryPolicy policy     = expiryPolicyFactory.create();

            Duration createDuration = policy.getExpiryForCreation();
            Duration updateDuration = policy.getExpiryForUpdate();
            Duration accessDuration = policy.getExpiryForAccess();

            TimeUnit createTimeUnit = createDuration == null ? null : createDuration.getTimeUnit();
            TimeUnit updateTimeUnit = updateDuration == null ? null : updateDuration.getTimeUnit();
            TimeUnit accessTimeUnit = accessDuration == null ? null : accessDuration.getTimeUnit();

            TimeSpan createTimeSpan = null;
            TimeSpan updateTimeSpan = null;
            TimeSpan accessTimeSpan = null;

            TimeSpan timeSpan = null;
            if(createTimeUnit != null) {
                createTimeSpan = TimeSpan.fromSeconds(createTimeUnit.toSeconds(createDuration.getDurationAmount()));
            }

            if(updateTimeUnit != null) {
                updateTimeSpan = TimeSpan.fromSeconds(updateTimeUnit.toSeconds(updateDuration.getDurationAmount()));
            }

            if(accessTimeUnit != null) {
                accessTimeSpan = TimeSpan.fromSeconds(accessTimeUnit.toSeconds(accessDuration.getDurationAmount()));
            }

            // use createTimespan
            if (createTimeSpan != null) {
                timeSpan = createTimeSpan;
                cacheCreatePolicy.setAbsoluteTimeout(true);
                timeoutType = "absolute";
            }

            // use absolute on read (i.e. timeout is reset when object is updated)
            if(timeSpan == null || updateTimeSpan != null) {
                timeSpan = timeSpan != null ? updateTimeSpan.getSeconds() > timeSpan.getSeconds() ? updateTimeSpan : timeSpan : updateTimeSpan;
                cacheCreatePolicy.setAbsoluteTimeoutOnRead(true);
                timeoutType = "absolute_on_read";
            }

            // use accessTimespan (i.e. timeout is always reset with any object access)
            if(timeSpan == null || accessTimeSpan != null) {
                timeSpan = timeSpan != null ? accessTimeSpan.getSeconds() > timeSpan.getSeconds() ? accessTimeSpan : timeSpan : accessTimeSpan;
                cacheCreatePolicy.setAbsoluteTimeout(false);
                cacheCreatePolicy.setAbsoluteTimeoutOnRead(false);
                timeoutType = "sliding";
            }

            if(timeSpan != null) {
                cacheCreatePolicy.setTimeout(timeSpan);
            }
        }

        _cache.setDefaultCreatePolicy(cacheCreatePolicy);

        _logger.info("Added default create policy.");
        _logger.info(String.format("TimeoutType: %s - Timeout: %s", timeoutType, cacheCreatePolicy.getTimeout().getSeconds()));

        // add expiration listeners
        for(CacheEntryListenerConfiguration<K,V> configuration : config.getCacheEntryListenerConfigurations()) {
            ScaleoutCacheExpirationListener listener = getCacheEntryExpiredListener(configuration);
            try {
                _cache.addListener(listener);
                _listeners.put(configuration, listener);
                _logger.info("Adding CacheEntryListener.");
                _logger.info(configuration.toString());
            } catch (NamedCacheException e) {
                throw new CacheException("Could not register for events.");
            }
        }

        // add backing store
        if(config.isReadThrough() || config.isWriteThrough()) {
            _logger.info("Adding ScaleOut backing store.");
            _logger.info(String.format("isReadThrough: %s isWriteThrough: %s", config.isReadThrough(), config.isWriteThrough()));
            Factory<CacheLoader<K,V>>                   loaderFactory = config.getCacheLoaderFactory();
            Factory<CacheWriter<? super K, ? super V>>  writerFactory = config.getCacheWriterFactory();
            CacheLoader<K,V>                    loader = null;
            CacheWriter<? super K, ? super V>   writer = null;
            if (loaderFactory != null) {
                loader = loaderFactory.create();
            }
            if(writerFactory != null) {
                writer = writerFactory.create();
            }
            ScaleoutCacheBackingStore store = new ScaleoutCacheBackingStore(this, loader, writer);
            try {
                _cache.setBackingStoreAdapter(store, new BackingStorePolicy(false, config.isReadThrough(), config.isWriteThrough()));
                _cache.setEventDeliveryExceptionHandler(new EventDeliveryExceptionHandler() {
                    @Override
                    public void eventDeliveryException(EventDeliveryExceptionHandlerArgs eventDeliveryExceptionHandlerArgs) {
                        _logger.severe("Missed event from ScaleOut cache.");
                    }
                });
            } catch (NamedCacheException e) {
                throw new CacheException("Could not add backing store adapter.");
            }
        }
    }

    /**
     * <p>
     *     Attempts to retrieve a value from this {@link Cache}. If the {@link Cache} contains a entry for this key then the
     *     associated value is returned otherwise null is returned.
     * </p>
     *
     * <p>
     *     If {@link Cache#isClosed()} returns true then an {@link IllegalStateException} is thrown.
     *     If the parameter key is null then a {@link NullPointerException} is thrown.
     *     If ScaleOut StateServer is unavailable, then a {@link CacheException} is thrown.
     *     If the configured value type does not match the value type of the object stored inside the {@link Cache} then a
     *     {@link ClassCastException} is thrown.
     * </p>

     * @param key the key used to find the key/value entry stored in the NamedCache
     * @return the value associated with the parameter key, or null
     * @throws CacheException if an exception occurs during retrieval, serialization of the user's key, or deserialization
     * of the user's object
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if the key is null
     */
    @Override
    public V get(K key) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        try {
            CachedObjectId<V> id = createCachedObjectId(key);
            Object retrieved = _cache.get(id);
            if(retrieved == null) return null;
            return _valueType.cast(retrieved);
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    /**
     * <p>
     *     Retrieves a map of key/value pairs for the parameter set of keys.
     * </p>
     *
     * <p>
     *     If a key does not have an associated value then no entry will exist in the returned map.
     * </p>
     *
     * @param set the set of keys to retrieve
     * @return a Map of Key/Value pairs
     * @throws CacheException if an exception occurs during retrieval, serialization of the user's key, or deserialization
     * of the user's object
     * @throws NullPointerException if a key in the set is null
     */
    @Override
    public Map<K, V> getAll(Set<? extends K> set) throws CacheException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        return getAll(false, set);
    }

    /**
     * <p>
     *     Checks to see if the {@link Cache} contains an entry for the parameter key.
     * </p>
     *
     * <p>
     *     <b>NOTE:</b> the underlying repository for this JSR107 implementation is a distributed NamedCache. Therefore, another
     *     client (or another thread in this application) could create/update or remove/clear the key <b>AFTER</b> this
     *     operation completes. Therefore, the result should not be treated as authoritative.
     * </p>
     *
     * @param key the key used to find the key/value entry stored in the NamedCache
     * @return true/false if a key/value entry exists
     * @throws CacheException if an exception occurs during serialization of the user's key or connectivity with the
     * ScaleOut StateServer store.
     * @throws IllegalStateException if the Cache is closed
     * @throws NullPointerException if the key is null
     */
    @Override
    public boolean containsKey(K key) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        try {
            CachedObjectId<V> id = createCachedObjectId(key);
            return _cache.contains(id);
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    /**
     * <p>
     *     Load all is not implemented and will throw an UnsupportedOperationException if the {@link Cache} is not closed.
     * </p>
     *
     * @param set the keys to load from the backing store
     * @param replaceExistingValues if true, the values are replaced, if false, the cached value is maintained
     * @param completionListener used to mark completion of the load operation
     * @throws IllegalStateException if the Cache is closed
     * @throws UnsupportedOperationException this operation is not supported
     */
    @Override
    public void loadAll(Set<? extends K> set, boolean replaceExistingValues, CompletionListener completionListener) throws IllegalStateException, UnsupportedOperationException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        throw new UnsupportedOperationException();
    }

    /**
     * <p>
     *     Associates the specified value with the specified key in the {@link Cache}.
     * </p>
     * <p>
     *     If the associated key/value pair already exists, then the entry is updated with the parameter value.
     * </p>
     *
     * @param key the key used to create a key/value entry in this cache
     * @param value the value associated with the parameter key
     * @throws CacheException if an exception occurs during put or during serialization of the user's key or value
     * @throws IllegalStateException if the Cache is closed
     * @throws NullPointerException if the key/value is null
     */
    @Override
    public void put(K key, V value) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        if(value == null) throw new NullPointerException("Value was null.");
        try {
            CachedObjectId<V> id = createCachedObjectId(key);
            _cache.put(id, value);
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        } catch (Exception e) {
            throw new CacheException(e);
        }

    }

    /**
     * <p>
     *     Associates the specified value with the specified key in this {@link Cache}, returning an existing value if one existed.
     * </p>
     *
     * <p>
     *     If the cache previously contained a entry for the parameter key, the previous value will be returned.
     *     If no value previously existed then null will be returned.
     * </p>
     *
     * @param key the key used to find the associated value
     * @param value the value to put associate with the parameter key
     * @return the previous value associated with the parameter key, or null
     * @throws CacheException if an exception occurs during get or put, during serialization of the user's key or value,
     * or during deserialization of the users stored value
     * @throws IllegalStateException if the Cache is closed
     * @throws NullPointerException if the key or value is null
     */
    @Override
    public V getAndPut(K key, V value) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        if(value == null) throw new NullPointerException("Value was null.");

        CachedObjectId<V> id;
        try {
            id = createCachedObjectId(key);
        } catch (Exception e) {
            throw new CacheException(e);
        }

        do {
            boolean locked = false; // used for cleanup in finally block.

            try {
                Object retrieved = _cache.retrieve(id, true);

                V ret = null;
                if (retrieved != null) {
                    locked = true;
                    ret = _valueType.cast(retrieved);
                    _cache.update(id, value, new UpdateOptions(UpdateLockingMode.UnlockAfterUpdate));
                    locked = false;
                } else {
                    _cache.insert(id, value, _cache.getDefaultCreatePolicy(), false, false);
                }

                return ret;
            } catch (NamedCacheObjectNotFoundException e) {
                // thrown from _cache.update call because another client/thread didn't
                // honor the lock and removed the obj right after our read. Retry.
                continue;
            } catch (NamedCacheObjectAlreadyExistsException e) {
                // thrown from _cache.insert call because another client/thread didn't
                // honor the lock and did a put right after our read. Retry.
                continue;
            } catch (NamedCacheException e) {
                throw new CacheException(e);
            } catch (ClassCastException cce) {
                StringBuilder builder = new StringBuilder("Could not return configured type: ");
                builder.append(_valueType);
                throw new ClassCastException(builder.toString());
            } finally {
                if (locked) {
                    try {
                        _cache.releaseLock(id);
                    } catch (NamedCacheException e) {
                        // this is just a best effort to clean up--do nothing.
                    }
                }
            }
        } while (true);
    }

    /**
     * <p>
     *     Puts all of the key/value pairs in the parameter map into the {@link Cache}.
     * </p>
     *
     * <p>
     *     If the {@link Cache} contains a previous entry, the value will be updated.
     * </p>
     * @param map a map of key/value pairs to put into the cache
     * @throws CacheException if an exception occurs during a put or serialization of the user's key or value
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if any of the keys or values are null
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> map) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        List<Future> futures = new LinkedList<Future>();
        for(Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            futures.add(_service.submit(() -> put(entry.getKey(), entry.getValue())));
        }
        for(Future f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                throw new CacheException(e);
            } catch (ExecutionException e) {
                throw new CacheException(e);
            }
        }
    }

    /**
     * <p>
     *     Puts a key/value pair into the {@link Cache}, if the {@link Cache} does not contain an entry for the key/value pair.
     * </p>
     * @param key the key used to create a key/value entry in this cache
     * @param value the value to put into the cache
     * @return true if the key/value pair was put into the cache, false if a previous entry exists
     * @throws CacheException if an exception occurs during put or serialization of the user's key or value
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if the key or value is null
     */
    @Override
    public boolean putIfAbsent(K key, V value) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        if(value == null) throw new NullPointerException("Value was null.");
        try {
            CachedObjectId<V> id = createCachedObjectId(key);
            _cache.insert(id, value, _cache.getDefaultCreatePolicy(), false, false);

            // if we get here then the object didn't already exist.
            return true;
        } catch (NamedCacheObjectAlreadyExistsException e) {
            // Object already exists. Swallow exception, return false.
            return false;
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    /**
     * <p>
     *     Removes the key/value entry from the {@link Cache}.
     * </p>
     *
     * @param key the key used to remove a key/value entry in this cache
     * @return true if the value was removed, false if the value was not removed.
     * @throws CacheException if an exception occurs during removal or serialization of the user's key
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if the key is null
     */
    @Override
    public boolean remove(K key) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        try {
            CachedObjectId<V> id = createCachedObjectId(key);
            _cache.remove(id);
            // Supposed to return false if there was no matching key, but
            // we have no way to determine that. Always return true.
            return true;
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        } catch (ClassCastException cce) {
            StringBuilder builder = new StringBuilder("Could not return configured type: ");
            builder.append(_keyType);
            throw new ClassCastException(builder.toString());
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    /**
     * <p>
     *     Removes the key/value pair from the {@link Cache} if the parameter value matches the retrieved value.
     * </p>
     *
     * <p>
     *     <b>NOTE:</b> Returns false if the key has no associated value.
     * </p>
     *
     * <p>
     *     <b>NOTE:</b> Equality is determined using the {@link Object#equals(Object)} method.
     * </p>
     * @param key the key used to remove a key/value entry in this cache
     * @param value the expected value in the cache
     * @return true if the key/value entry was removed, false if the object doesn't exist or the expected value doesn't match
     * the retrieved value
     * @throws CacheException if an exception occurs during removal or serialization of the user's key or value
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if the key or value is null
     */
    @Override
    public boolean remove(K key, V value) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        if(value == null) throw new NullPointerException("Value was null.");

        CachedObjectId<V> id;
        try {
            id = createCachedObjectId(key);
        } catch (Exception e) {
            throw new CacheException(e);
        }

        boolean locked = false;

        try {
            Object retrieved = _cache.retrieve(id, true);

            if(retrieved == null) {
                return false;
            }
            else {
                locked = true;
            }

            V ret = null;
            ret = _valueType.cast(retrieved);
            if(value.equals(ret)) {
                _cache.remove(id);
                locked = false;
                return true;
            } else {
                // obj will be unlocked in finally block.
                return false;
            }
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        } catch (ClassCastException cce) {
            StringBuilder builder = new StringBuilder("Could not return configured type: ");
            builder.append(_valueType);
            throw new ClassCastException(builder.toString());
        } catch (Exception e) {
            throw new CacheException(e);
        } finally {
            if (locked) {
                try {
                    _cache.releaseLock(id);
                } catch (NamedCacheException e) {
                    // this is just a best effort to clean up--do nothing.
                }
            }
        }
    }

    /**
     * <p>
     *     If the entry exists for the parameter key, the entry is removed and the previous value is returned.
     * </p>
     * @param key the key used to find the key/value entry in this cache
     * @return the previous value or null if no key/value entry existed
     * @throws CacheException if an exception occurs during get, removal of the entry, or serialization of the user's
     * key, or deserialization of the user's value
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if the key is null
     */
    @Override
    public V getAndRemove(K key) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");

        CachedObjectId<V> id;
        try {
            id = createCachedObjectId(key);
        } catch (Exception e) {
            throw new CacheException(e);
        }

        boolean locked = false;

        try {
            Object retrieved = _cache.retrieve(id, true);
            if(retrieved == null) {
                return null;
            } else {
                locked = true;
            }

            V ret = _valueType.cast(retrieved);
            _cache.remove(id);
            locked = false;
            return ret;
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        } catch (ClassCastException cce) {
            StringBuilder builder = new StringBuilder("Could not return configured type: ");
            builder.append(_valueType);
            throw new ClassCastException(builder.toString());
        } catch (Exception e) {
            throw new CacheException(e);
        } finally {
            if (locked) {
                try {
                    _cache.releaseLock(id);
                } catch (NamedCacheException e) {
                    // this is just a best effort to clean up--do nothing.
                }
            }
        }
    }

    /**
     * <p>
     *     Replaces a key/value pair with a new value if the current cached value is identical to the parameter value.
     * </p>
     *
     * <p>
     *     <b>NOTE:</b> Equality is determined using the {@link Object#equals(Object)} method.
     * </p>
     *
     * @param key the key to use to find the existing key/value entry
     * @param oldValue the old value to compare the existing value with
     * @param newValue the new value to update the existing value with
     * @return true/false if the entry was updated or false if the entry is unchanged -- null, or oldValue != cachedValue
     * @throws CacheException if an exception occurs while retrieving or updating the entry, or serialization of the user's
     * key/value, or deserialization of the user's value
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if the key or values are null
     */
    @Override
    public boolean replace(K key, V oldValue, V newValue) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        if(oldValue == null) throw new NullPointerException("oldValue was null.");
        if(newValue == null) throw new NullPointerException("newValue was null.");

        CachedObjectId<V> id;
        try {
            id = createCachedObjectId(key);
        } catch (Exception e) {
            throw new CacheException(e);
        }

        do {
            boolean locked = false;
            try {
                Object retrieved = _cache.retrieve(id, true);
                if (retrieved == null)
                    return false;
                else
                    locked = true;

                V ret = _valueType.cast(retrieved);
                if (oldValue.equals(ret)) {
                    _cache.update(id, newValue, new UpdateOptions(UpdateLockingMode.UnlockAfterUpdate));
                    locked = false;
                    return true;
                } else {
                    // finally block will release lock.
                    return false;
                }
            } catch (NamedCacheObjectNotFoundException e) {
                // thrown from _cache.update call because another client/thread didn't
                // honor the lock and removed the obj right after our read. Retry.
                continue;
            } catch (NamedCacheException e) {
                throw new CacheException(e);
            } catch (ClassCastException cce) {
                StringBuilder builder = new StringBuilder("Could not return configured type: ");
                builder.append(_valueType);
                throw new ClassCastException(builder.toString());
            } catch (Exception e) {
                throw new CacheException(e);
            } finally {
                if (locked) {
                    try {
                        _cache.releaseLock(id);
                    } catch (NamedCacheException e) {
                        // this is just a best effort to clean up--do nothing.
                    }
                }
            }
        } while (true);

    }

    /**
     * <p>
     *     Replaces the value of a key/value pair stored in the {@link Cache}.
     * </p>
     *
     * <p>
     *     If the key/value pair doesn't exist in the {@link Cache} then the key/value pair won't be added to the {@link Cache}.
     * </p>
     * @param key the key to use for finding the existing key/value entry
     * @param value the value used to replace the existing value
     * @return true if the existing value was updated with the new value, false if no key/value entry existed
     * @throws CacheException if an exception occurs while retrieving or updating the entry, or serialization of the user's
     * key/value, or deserialization of the user's value
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if the key or value is null
     */
    @Override
    public boolean replace(K key, V value) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        if(value == null) throw new NullPointerException("Value was null.");

        CachedObjectId<V> id;
        try {
            id = createCachedObjectId(key);
        } catch (Exception e) {
            throw new CacheException(e);
        }

        try {
            _cache.update(id, value, new UpdateOptions(UpdateLockingMode.None));
            return true;
        } catch (NamedCacheObjectNotFoundException e) {
            // object didn't exist
            return false;
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        } catch (ClassCastException cce) {
            StringBuilder builder = new StringBuilder("Could not return configured type: ");
            builder.append(_valueType);
            throw new ClassCastException(builder.toString());
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    /**
     * <p>
     *     Gets a key/value pair entry and replaces the value.
     * </p>
     *
     * <p>
     *     If the key/value entry doesn't exist, then the new key/value entry is not created.
     * </p>
     * @param key the key to use to find the key/value entry
     * @param value the value used to replace the existing value
     * @return true if the value was updated, false if the key/value entry doesn't exist
     * @throws CacheException if an exception occurs while retrieving or updating the entry, or serialization of the user's
     * key/value, or deserialization of the user's value
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if the key or value is null
     */
    @Override
    public V getAndReplace(K key, V value) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(key == null) throw new NullPointerException("Key was null.");
        if(value == null) throw new NullPointerException("Value was null.");

        CachedObjectId<V> id;
        try {
            id = createCachedObjectId(key);
        } catch (Exception e) {
            throw new CacheException(e);
        }

        boolean locked = false;

        do {
            try {
                Object retrieved = _cache.retrieve(id, true);
                if (retrieved == null) {
                    return null; // object didn't exist, don't do update.
                } else {
                    locked = true;
                }

                V ret = _valueType.cast(retrieved);

                _cache.update(id, value, new UpdateOptions(UpdateLockingMode.UnlockAfterUpdate));
                locked = false;
                return ret;
            } catch (NamedCacheObjectNotFoundException e) {
                // thrown from _cache.update call because another client/thread didn't
                // honor the lock and removed the obj right after our read. Retry.
                continue;
            } catch (NamedCacheException e) {
                throw new CacheException(e);
            } catch (ClassCastException cce) {
                StringBuilder builder = new StringBuilder("Could not return configured type: ");
                builder.append(_valueType);
                throw new ClassCastException(builder.toString());
            } catch (Exception e) {
                throw new CacheException(e);
            } finally {
                if (locked) {
                    try {
                        _cache.releaseLock(id);
                    } catch (NamedCacheException e) {
                        // this is just a best effort to clean up--do nothing.
                    }
                }
            }
        } while (true);
    }

    /**
     * <p>
     *     Removes all of the key/value entries based on the parameter set of keys.
     * </p>
     * @param set the keys used to find and remove key/value entries from this cache
     * @throws CacheException if an exception occurs while removing or serialization of the user's key
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if any of the keys in the set are null
     */
    @Override
    public void removeAll(Set<? extends K> set) throws CacheException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        List<Future> futures = new LinkedList<Future>();
        for(K key : set) {
            futures.add(_service.submit(()->
                    remove(key)
            ));
        }
        for(Future f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                throw new CacheException(e);
            } catch (ExecutionException e) {
                throw new CacheException(e);
            }
        }
    }

    /**
     * <p>
     *     Removes all key/value entries from the {@link Cache}.
     * </p>
     * @throws CacheException if an exception occurs while removing all values
     * @throws IllegalStateException if the cache is closed
     */
    @Override
    public void removeAll() throws CacheException, IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        try {
            _cache.clear();
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        }
    }

    /**
     * <p>
     *     Clears all key/value entries from the {@link Cache}.
     * </p>
     * @throws CacheException if an exception occurs while cleaing all values
     * @throws IllegalStateException if the cache is closed
     */
    @Override
    public void clear() throws CacheException, IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        try {
            _cache.clear();
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        }
    }

    /**
     * <p>
     *     Returns the underlying configuration class for this {@link Cache}.
     * </p>
     * @param aClass the configuration class type to return
     * @param <C> the returned configuration class type
     * @return the configuration of this class cast as type C
     * @throws IllegalStateException if the cache is closed
     */
    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> aClass) throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        return aClass.cast(_config);
    }

    /**
     * <p>
     *     Runs the parameter entry processor on a key/value entry.
     * </p>
     * @param k the key used to find the key/value entry
     * @param entryProcessor the entry processor used on the key/value entry
     * @param objects the set of objects to pass to the entry processor
     * @param <T> the object type to return
     * @return the result returned by the entry processor
     * @throws EntryProcessorException if the entry processor encounters an exception
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if the key is null
     */
    @Override
    public <T> T invoke(K k, EntryProcessor<K, V, T> entryProcessor, Object... objects)
            throws EntryProcessorException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        V v = get(k);
        ScaleoutCacheEntry entry = new ScaleoutCacheEntry(k, v, this);
        return entryProcessor.process(entry, objects);
    }

    /**
     * <p>
     *     Runs the parameter entry processor on the set of key/value entries.
     * </p>
     * @param set they keys used to find key/value entries
     * @param entryProcessor the entry processor used on each key/value entry
     * @param objects the objects to pass to the entry processor
     * @param <T> the object type to return
     * @return a map of keys to entry processor results of type T
     * @throws EntryProcessorException if one or more entries encounter an exception
     * @throws IllegalStateException if the cache is closed
     * @throws NullPointerException if any of the keys in the set are null
     */
    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> set, EntryProcessor<K, V, T> entryProcessor, Object... objects)
            throws EntryProcessorException, IllegalStateException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        Map<K,V> entries = getAll(true, set);
        Map<K, EntryProcessorResult<T>> ret = new HashMap<>(entries.size());
        for(Map.Entry<K,V> entry : entries.entrySet()) {

            ScaleoutCacheEntry sce = new ScaleoutCacheEntry(entry.getKey(), entry.getValue(), this);
            T result = entryProcessor.process(sce, objects);
            ret.put(entry.getKey(), new EntryProcessorResult<T>() {
                @Override
                public T get() throws EntryProcessorException {
                    return result;
                }
            });
        }
        return ret;
    }

    /**
     * <p>
     *     Returns the name of the cache.
     * </p>
     * @return the name of this cache
     * @throws IllegalStateException if the cache is closed
     */
    @Override
    public String getName() throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        return _config.getCacheName();
    }

    /**
     * <p>
     *     Returns the {@link CacheManager} that manages this {@link Cache}.
     * </p>
     * @return the cache manager managing this cache
     * @throws IllegalStateException if the cache is closed
     */
    @Override
    public CacheManager getCacheManager() throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        return _manager;
    }

    /**
     * <p>
     *     Closes this {@link Cache} for any further operations.
     * </p>
     */
    @Override
    public void close() throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        _closed = true;
        if(_listeners != null) {
            for(NamedCacheObjectExpirationListener listener : _listeners.values()) {
                try {
                    _cache.removeListener(listener);
                } catch (NamedCacheException e) {
                    throw new CacheException("Could not remove listener.", e);
                }
            }
            _listeners.clear();
        }
        _manager.removeNamespace(_config.getCacheName());
    }

    /**
     * <p>
     *     Check to see if this {@link Cache} is closed.
     * </p>
     * @return true if the cache is closed, false if the cache is open
     */
    @Override
    public boolean isClosed() {
        return _closed;
    }

    /**
     * <p>
     *     Provides a standard way to access the underlying concrete caching implementation to provide access to further, proprietary features.
     * </p>
     * @param aClass the class of the cache
     * @param <T> the type of the cache
     * @return the underlying cache implementation
     * @throws IllegalStateException if the cache is closed
     */
    @Override
    public <T> T unwrap(Class<T> aClass) throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        return aClass.cast(this);
    }

    /**
     * <p>
     *     Registers a {@link CacheEntryExpiredListener} with this {@link Cache}. Expiration events are handled asynchronously by the
     *     implementation provided by the parameter {@link CacheEntryListenerConfiguration}.
     * </p>
     *
     * <p>
     *     <b>NOTE:</b> this {@link Cache} implementation only supports {@link CacheEntryExpiredListener} and all
     *     other {@link CacheEntryListenerConfiguration} will throw an {@link UnsupportedOperationException}.
     * </p>
     *
     * <p>
     *     <b>NOTE:</b> ScaleOut StateServer uses a scalable expiration model -- meaning expiration events are not
     *     necessarily handled by the application that registers the {@link CacheEntryExpiredListener}. More info on
     *     expiration events can be found in the ScaleOut StateServer help file.
     * </p>
     * @param cacheEntryListenerConfiguration the configuration used to create a CacheEntryListener
     * @throws CacheException if an exception occurs while registering an event listener
     * @throws IllegalStateException if the cache is closed
     * @throws UnsupportedOperationException if the configuration is for any event listener other than expiration events
     * @throws NullPointerException if the parameter configuration is null
     */
    @Override
    @SuppressWarnings("unchecked")
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration)
            throws CacheException, IllegalStateException, UnsupportedOperationException, NullPointerException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(cacheEntryListenerConfiguration == null) throw new NullPointerException("CacheEntryListenerConfiguration was null.");
        ScaleoutCacheExpirationListener listener = getCacheEntryExpiredListener(cacheEntryListenerConfiguration);
        try {
            _cache.addListener(listener);
            _listeners.put(cacheEntryListenerConfiguration, listener);
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        }
    }

    /**
     * <p>
     *     Deregisters a {@link CacheEntryExpiredListener} from this {@link Cache}.
     * </p>
     *
     * <p>
     *     <b>NOTE:</b> this {@link Cache} implementation only supports {@link CacheEntryExpiredListener} and all
     *     other {@link CacheEntryListenerConfiguration} will throw an {@link UnsupportedOperationException}.
     * </p>
     * @param cacheEntryListenerConfiguration the configuration used to deregister an event listener
     * @throws CacheException if an exception occurs while deregistering an event listener
     * @throws IllegalStateException if the cache is closed
     * @throws UnsupportedOperationException if the configuration is for any event listener other than expiration events
     * @throws NullPointerException if the parameter configuration is null
     */
    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration)
            throws CacheException, IllegalStateException, UnsupportedOperationException {
        if(_closed) {
            throw new IllegalStateException("Cache is closed.");
        }
        if(cacheEntryListenerConfiguration == null) throw new NullPointerException("CacheEntryListenerConfiguration was null.");
        ScaleoutCacheExpirationListener listener = getCacheEntryExpiredListener(cacheEntryListenerConfiguration);
        try {
            NamedCacheObjectExpirationListener temp = _listeners.remove(cacheEntryListenerConfiguration);
            _cache.removeListener(temp);
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        }
    }

    /**
     * <p>
     *     Returns an iterator of all key/value entries in this {@link Cache}.
     * </p>
     *
     * <p>
     *     <b>NOTE:</b> all {@link Iterator#next()} method calls perform a full key/value entry retrieval.
     * </p>
     * @return a lazy iterator for all key/value entries in this cache
     * @throws CacheException if an exception occurs while iterating over the entries in the cache
     */
    @Override
    public Iterator<Entry<K, V>> iterator() throws CacheException {
        try {
            Set<CachedObjectId<V>> keys = _cache.queryKeys(_valueType, null);
            return new NamedCacheIterator(keys, _cache,this);
        } catch (NamedCacheException e) {
            throw new CacheException(e);
        }
    }

    /**
     * private helper method to retrieve a set of keys returning null values if the key doesn't have a mapped
     * value in the store. We need key/value pairs with null values for invoke all.
     * @param returnKeysWithNullValues true or false to return null values
     * @param set the set of keys to get
     * @return a set of key/value entries
     */
    private Map<K,V> getAll(boolean returnKeysWithNullValues, Set<? extends K> set) {
        final Map<K, V> result = new HashMap<K, V>(set.size());
        List<Future> futures = new LinkedList<Future>();
        for(K key : set) {
            futures.add(_service.submit(()-> {
                        V val = get(key);
                        if(val != null || returnKeysWithNullValues) {
                            synchronized (result) {
                                result.put(key, val);
                            }
                        }
                    })
            );
        }

        for(Future f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                throw new CacheException(e);
            } catch (ExecutionException e) {
                throw new CacheException(e);
            } catch (Exception e) {
                continue;
            }
        }
        return result;
    }

    /**
     * Private helper method to create a CachedObjectId -- a key in the NamedCache.
     * @param key the user key to base the CachedObjectId on
     * @return a CachedObjectId
     * @throws IOException
     * @throws NamedCacheException
     * @throws ObjectNotSupportedException
     */
    private CachedObjectId<V> createCachedObjectId(K key) throws IOException, NamedCacheException, ObjectNotSupportedException {
        String sKey = null;
        byte[] bytes = serializeKey(key);
        sKey = Base64.getEncoder().encodeToString(bytes);
        return _cache.createKey(sKey);
    }

    /**
     * Private helper method to serialize a user's key into a byte[]
     * @param key they user key to serialize
     * @return a byte[] of the user's key
     * @throws IOException
     * @throws ObjectNotSupportedException
     */
    private byte[] serializeKey(K key) throws IOException, ObjectNotSupportedException {
        ByteArrayOutputStream baos  = new ByteArrayOutputStream();
        ObjectOutputStream oos      = new ObjectOutputStream(baos);
        if(_keySerializer != null) {
            _keySerializer.serialize(oos, key);
        } else {
            oos.writeObject(key);
        }

        oos.flush();
        oos.close();
        baos.flush();
        baos.close();
        return baos.toByteArray();
    }

    /**
     * Private helper method to deserialize a byte[] into a user's key.
     * @param bytes the serialized key
     * @return a typed user's key
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws ObjectNotSupportedException
     */
    private K deserializeKey(byte[] bytes) throws IOException, ClassNotFoundException, ObjectNotSupportedException {
        ByteArrayInputStream bais   = new ByteArrayInputStream(bytes);
        ObjectInputStream ois       = new ObjectInputStream(bais);
        Object o;
        if (_keySerializer != null) {
            o = _keySerializer.deserialize(ois);
        } else {
            o = ois.readObject();
        }
        ois.close();
        bais.close();
        return _keyType.cast(o);
    }

    private ScaleoutCacheExpirationListener getCacheEntryExpiredListener(CacheEntryListenerConfiguration<K,V> cacheEntryListenerConfiguration) throws UnsupportedOperationException {
        Factory<CacheEntryListener<? super K, ? super V>> factory = cacheEntryListenerConfiguration.getCacheEntryListenerFactory();
        CacheEntryListener<? super K, ? super V> listener = factory.create();
        if(listener instanceof CacheEntryExpiredListener) {
            CacheEntryExpiredListener<K,V> expiryListener = (CacheEntryExpiredListener<K,V>) listener;
            return new ScaleoutCacheExpirationListener(this, expiryListener);
        } else {
            throw new UnsupportedOperationException("Listener configuration must be for object expiration. Configuration type: " + listener.getClass().getName());
        }
    }

    /**
     * Private iterator class that lazily retrieves objects from a NamedCache.
     * This class is used for the {@link Cache#iterator()} method.
     */
    class NamedCacheIterator implements Iterator<Entry<K,V>> {

        Iterator<CachedObjectId<V>> _ids;
        transient NamedCache _underlyingCache;
        transient Cache<K,V> _cache;

        public NamedCacheIterator(Set<CachedObjectId<V>> ids, NamedCache underlyingCache, Cache<K,V> cache) {
            _ids = ids.iterator();
            _underlyingCache = underlyingCache;
            _cache = cache;
        }

        public boolean hasNext() {
            return _ids.hasNext();
        }

        public Entry<K,V> next() {
            CachedObjectId<V> id = _ids.next();
            byte[] serializedKey = new byte[0];
            try {
                String sKey = id.getKeyString();
                serializedKey = Base64.getDecoder().decode(sKey);
            } catch (Exception e) {
                throw new CacheException(e);
            }
            K key = null;
            V val = null;
            try {
                key = deserializeKey(serializedKey);
            } catch (IOException | ClassNotFoundException | ObjectNotSupportedException e) {
                throw new CacheException(e);
            }
            try {
                val = _underlyingCache.get(id);
            } catch (NamedCacheException e) {
                throw new CacheException(e);
            }

            return new ScaleoutCacheEntry(key, val, _cache);
        }
    }

    /**
     * Private mutable entry implementation that wraps a key/value potentially stored in a NamedCache
     */
    class ScaleoutCacheEntry implements MutableEntry<K,V> {

        private K _key;
        private V _value;
        transient Cache<K,V> _cache;

        public ScaleoutCacheEntry(K key, V value, Cache<K,V> cache) {
            _key = key;
            _value = value;
            _cache = cache;
        }

        @Override
        public boolean exists() {
            return _key == null || _value == null;
        }

        @Override
        public void remove() {
            if(exists()) {
                _cache.remove(_key);
            }
        }

        @Override
        public K getKey() {
            return _key;
        }

        @Override
        public V getValue() {
            return _value;
        }

        @Override
        public <T> T unwrap(Class<T> aClass) {
            return aClass.cast(this);
        }

        @Override
        public void setValue(V v) {
            _value = v;
            _cache.put(_key, _value);
        }
    }

    /**
     * An implementation of a NamedCacheObjectExpirationListener that turns a NamedCache expiration event into a
     * {@link CacheEntryEvent} and runs a user defined callback. The user defined callback is registered using this
     * {@link Cache}'s expiration event listener defined using {@link Cache#registerCacheEntryListener(CacheEntryListenerConfiguration)}.
     */
    class ScaleoutCacheExpirationListener implements NamedCacheObjectExpirationListener {
        Cache<K,V> _cache;
        CacheEntryExpiredListener<K, V> _listener;

        ScaleoutCacheExpirationListener(Cache<K,V> cache, CacheEntryExpiredListener<K, V> listener) {
            _cache = cache;
            _listener = listener;
        }

        @Override
        public void objectExpired(NamedCacheObjectExpiredEventArgs args) {
            CachedObjectId id = args.getCachedObjectId();
            K key;
            V value;
            byte[] serializedKey = new byte[0];
            try {
                String sKey = id.getKeyString();
                serializedKey = Base64.getDecoder().decode(sKey);
            } catch (Exception e) {
                throw new CacheException(e);
            }
            try {
                key = deserializeKey(serializedKey);
            } catch (IOException | ClassNotFoundException | ObjectNotSupportedException e) {
                throw new CacheException(e);
            }

            try {
                Object o = args.getNamedCache().get(id);
                value = _valueType.cast(o);
            } catch (NamedCacheException e) {
                throw new CacheException(e);
            }

            List<CacheEntryEvent<? extends K,? extends V>> list = new LinkedList<>();
            list.add(new ScaleoutCacheEntryEvent(_cache, EventType.EXPIRED, key, value));
            _listener.onExpired(list);
        }
    }

    /**
     * Internal cache entry event implementation used to translate a NamedCache expiration event into a
     * {@link CacheEntryEvent}.
     */
    class ScaleoutCacheEntryEvent extends CacheEntryEvent<K, V> {
        K _key;
        V _value;
        public ScaleoutCacheEntryEvent(Cache<K,V> source, EventType eventType, K key, V value) {
            super(source, eventType);
            _key = key;
            _value = value;
        }

        @Override
        public V getValue() {
            return _value;
        }

        @Override
        public V getOldValue() {
            return null;
        }

        @Override
        public boolean isOldValueAvailable() {
            return false;
        }

        @Override
        public K getKey() {
            return _key;
        }

        @Override
        public <T> T unwrap(Class<T> aClass) {
            return aClass.cast(this);
        }
    }

    /**
     * Internal BackingStore implementation to support ReadThrough and WriteThrough operations.
     *
     * If supplied, the backing store implementation uses the {@link CacheLoader} and/or {@link CacheWriter} provider by
     * the user through a {@link CompleteConfiguration}.
     */
    class ScaleoutCacheBackingStore implements BackingStore {
        private final Cache<K,V>                        _callback;
        private final CacheLoader<K,V>                  _loader;
        private final CacheWriter<? super K,? super V>  _writer;

        public ScaleoutCacheBackingStore(Cache<K,V> cache, CacheLoader<K,V> loader, CacheWriter<? super K,? super V> writer) {
            _callback   = cache;
            _loader     = loader;
            _writer     = writer;
        }


        @Override
        public <T> T load(CachedObjectId<T> id) {
            if(_loader == null) return null;
            byte[] serializedKey = new byte[0];
            try {
                String sKey = id.getKeyString();
                serializedKey = Base64.getDecoder().decode(sKey);
            } catch (Exception e) {
                throw new CacheException(e);
            }
            K key;
            try {
                key = deserializeKey(serializedKey);
            } catch (IOException e) {
                throw new CacheException("IOException occurred while deserializing key.", e);
            } catch (ClassNotFoundException e) {
                throw new CacheException("ClassNotFoundException occurred while deserializing key.", e);
            } catch (ObjectNotSupportedException e) {
                throw new CacheException("ObjectNotSupportedException occurred while deserializing key.", e);
            } catch (Exception e) {
                throw new CacheException("Exception occurred while deserializing key.", e);
            }
            try {
                V value = _loader.load(key);
                return (T)value;
            } catch(Exception e) {
                throw new CacheException("Unexpected exception while loading value.", e);
            }
        }

        @Override
        public <T> void store(CachedObjectId<T> id, T obj) {
            if(_writer == null) return;
            byte[] serializedKey = new byte[0];
            try {
                String sKey = id.getKeyString();
                serializedKey = Base64.getDecoder().decode(sKey);
            } catch (Exception e) {
                throw new CacheException(e);
            }
            K key;
            V val;
            try {
                key = deserializeKey(serializedKey);
            } catch (IOException e) {
                throw new CacheException("IOException occurred while deserializing key.", e);
            } catch (ClassNotFoundException e) {
                throw new CacheException("ClassNotFoundException occurred while deserializing key.", e);
            } catch (ObjectNotSupportedException e) {
                throw new CacheException("ObjectNotSupportedException occurred while deserializing key.", e);
            } catch (Exception e) {
                throw new CacheException("Exception occurred while deserializing key.", e);
            }
            try {
                val = _valueType.cast(obj);
            } catch (Exception e) {
                String msg = String.format("Could not cast value to expected type (expected: %s - actual: %s.", _valueType.toString(), obj.getClass().toString());
                throw new CacheException(msg, e);
            }

            try {
                _writer.write(new ScaleoutCacheEntry(key, val, _callback));
            } catch(Exception e) {
                throw new CacheException("Unexpected exception thrown from CacheWriter.", e);
            }
        }

        @Override
        public void erase(CachedObjectId id) {
            if(_writer == null) return;
            byte[] serializedKey = new byte[0];
            try {
                String sKey = id.getKeyString();
                serializedKey = Base64.getDecoder().decode(sKey);
            } catch (Exception e) {
                throw new CacheException(e);
            }
            K key;
            try {
                key = deserializeKey(serializedKey);
            } catch (IOException e) {
                throw new CacheException("IOException occurred while deserializing key.", e);
            } catch (ClassNotFoundException e) {
                throw new CacheException("ClassNotFoundException occurred while deserializing key.", e);
            } catch (ObjectNotSupportedException e) {
                throw new CacheException("ObjectNotSupportedException occurred while deserializing key.", e);
            } catch (Exception e) {
                throw new CacheException("Exception occurred while deserializing key.", e);
            }
            try {
                _writer.delete(key);
            } catch(Exception e) {
                throw new CacheException("Unexpected exception thrown from CacheWriter.", e);
            }
        }

        @Override
        public CreatePolicy getCreatePolicy(CachedObjectId cachedObjectId) {
            return _cache.getDefaultCreatePolicy();
        }
    }
}
