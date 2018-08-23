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

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.*;

/**
 * {@link CacheManager} implementation used to create {@link ScaleoutCache}s.
 */
public class ScaleoutCacheManager implements CacheManager {

    private CachingProvider _provider;
    private Properties _properties;
    private URI _uri;
    private ClassLoader _classLoader;
    private HashMap<String, Cache<?,?>> _createdCaches;
    private List<String> _namespaces;
    private boolean _closed;

    /**
     * Instantiates a {@link ScaleoutCacheManager}.
     * @param provider the caching provider
     * @param uri the uri of the CacheManager
     * @param properties the properties of the CachingManager
     * @param classLoader the classloader for this CachingManager
     */
    ScaleoutCacheManager(CachingProvider provider, URI uri, Properties properties, ClassLoader classLoader) {
        _provider = provider;
        _properties = properties;
        _uri = uri;
        _classLoader = classLoader;
        _createdCaches = new HashMap<String, Cache<?,?>>();
        _namespaces = new LinkedList<>();
        _closed = false;
    }

    /**
     * Returns the caching provider for this {@link ScaleoutCacheManager}.
     * @return the caching provider for this {@link ScaleoutCacheManager}
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    public CachingProvider getCachingProvider() throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        return _provider;
    }

    /**
     * Returns the URI for this {@link ScaleoutCacheManager}.
     * @return the URI for this {@link ScaleoutCacheManager}
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    public URI getURI() throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        return _uri;
    }

    /**
     * Returns the classloader for this {@link ScaleoutCacheManager}.
     * @return the classloader for this {@link ScaleoutCacheManager}
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    public ClassLoader getClassLoader() throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        return _classLoader;
    }

    /**
     * Returns the properties of this {@link ScaleoutCacheManager}.
     * @return the properties of this {@link ScaleoutCacheManager}
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    public Properties getProperties() throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        return _properties;
    }

    /**
     * Creates and returns a new {@link ScaleoutCache}.
     * @param name the name to use for the {@link ScaleoutCache}
     * @param config the config to use for the {@link ScaleoutCache}
     * @param <K> the key type to use
     * @param <V> the value type to use
     * @param <C> the configuration type to use
     * @return a new {@link ScaleoutCache}
     * @throws IllegalStateException if the {@link ScaleoutCacheManager} is closed
     */
    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String name, C config) throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        Cache<K,V> ret = new ScaleoutCache<K, V>(new ScaleoutCacheConfiguration<K,V>(name, config), this);
        _createdCaches.put(name, ret);
        _namespaces.add(name);
        return ret;
    }

    /**
     * Returns a {@link ScaleoutCache}.
     * @param s the name of the ScaleoutCache
     * @param keyClass the key class to use for key/value entries
     * @param valueClass the value class to use for key/value entries
     * @param <K> the key type
     * @param <V> the value type
     * @return a {@link ScaleoutCache}
     * @throws IllegalStateException if the {@link ScaleoutCacheManager} is closed
     */
    @Override
    public <K, V> Cache<K, V> getCache(String s, Class<K> keyClass, Class<V> valueClass) throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        Cache<K,V> ret = new ScaleoutCache<K, V>(new ScaleoutCacheConfiguration<K,V>(s, keyClass,valueClass), this);
        _namespaces.add(s);
        _createdCaches.put(s, ret);
        return ret;
    }

    /**
     * Returns a {@link ScaleoutCache}.
     * @param s the name of the ScaleoutCache
     * @param <K> the key type
     * @param <V> the value type
     * @return a {@link ScaleoutCache}
     * @throws IllegalStateException if the {@link ScaleoutCacheManager} is closed
     */
    @Override
    public <K, V> Cache<K, V> getCache(String s) throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        K key = (K)new Object();
        Class<?> keyClass = key.getClass();
        V value = (V)new Object();
        Class<?> valueClass = value.getClass();
        Cache<K,V> ret = new ScaleoutCache<K, V>(new ScaleoutCacheConfiguration<K,V>(s, (Class<K>)keyClass,(Class<V>)valueClass), this);
        _namespaces.add(s);
        _createdCaches.put(s, ret);
        return ret;
    }

    /**
     * Returns all {@link ScaleoutCache}s created by this {@link ScaleoutCacheManager}.
     * @return a iterable of cache names
     * @throws IllegalStateException if the {@link ScaleoutCacheManager} is closed
     */
    @Override
    public Iterable<String> getCacheNames() throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        return _namespaces;
    }

    /**
     * Destroys a cache; clearing all contents and removing from internal bookkeeping.
     * @param s the cache to destry
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    public void destroyCache(String s) throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        Cache toDelete = _createdCaches.remove(s);
        toDelete.clear();
        toDelete.close();
    }

    /**
     * Enables management of a {@link ScaleoutCache}. Currently unsupported as noop.
     * @param name the name of the cache
     * @param flag true/false to enable/disable management
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    public void enableManagement(String name, boolean flag) throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
    }

    /**
     * Enables statistics of a {@link ScaleoutCache}. Currently unsupported as noop.
     * @param name the name of the cache
     * @param flag true/false to enable/disable statistics
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    public void enableStatistics(String name, boolean flag) throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
    }

    /**
     * Closes this {@link ScaleoutCacheManager}.
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    public void close() throws IllegalStateException {
        for(Map.Entry<String, Cache<?,?>> entry : _createdCaches.entrySet()) {
            entry.getValue().close();
        }
        _closed = true;
    }

    /**
     * Checks to see if this {@link ScaleoutCacheManager} is closed.
     * @return true/false if this {@link ScaleoutCacheManager} is closed
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    public boolean isClosed() throws IllegalStateException {
        return _closed;
    }

    /**
     * Unwraps this {@link ScaleoutCacheManager}.
     * @param aClass the class
     * @param <T> the type of {@link CacheManager}
     * @return the unwrapped {@link ScaleoutCacheManager}
     * @throws IllegalStateException if this {@link ScaleoutCacheManager} is closed
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> aClass) throws IllegalStateException {
        if(_closed) {
            throw new IllegalStateException("CacheManager is closed.");
        }
        return aClass.cast(this);
    }

    /**
     * private helper method to remove a cache from this CacheManager (occurs when a {@link ScaleoutCache#close()}
     * method is called.
     * @param s the cache to remove
     */
    void removeNamespace(String s) {
        _namespaces.remove(s);
        _createdCaches.remove(s);
    }
}
