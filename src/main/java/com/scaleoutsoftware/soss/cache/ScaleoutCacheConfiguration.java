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

import javax.cache.configuration.*;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import java.util.Collections;

/**
 * {@link Configuration} implementation used to configure a {@link ScaleoutCache}.
 * @param <K> the type of the key in an entry stored in the {@link ScaleoutCache}
 * @param <V> the type of the value in an entry stored in the {@link ScaleoutCache}
 */
public class ScaleoutCacheConfiguration<K,V> implements CompleteConfiguration<K,V> {

    private Class<K>                                        _keyType;
    private Class<V>                                        _valueType;
    private String                                          _cacheName;
    private Iterable<CacheEntryListenerConfiguration<K,V>>  _cacheEntryListenerConfigurations   = null;
    private Factory<ExpiryPolicy>                           _expiryPolicyFactory                = null;
    private Factory<CacheWriter<? super K,? super V>>       _cacheWriterFactory                 = null;
    private Factory<CacheLoader<K,V>>                       _cacheLoaderFactory                 = null;
    private boolean                                         _isReadThrough                      = false;
    private boolean                                         _isWriteThrough                     = false;

    /**
     * Instantiates a new configuration for a {@link ScaleoutCache}.
     * @param cacheName the cache name
     * @param keyType the key type for key/value entries
     * @param valueType the value type for key/value entries
     */
    ScaleoutCacheConfiguration(String cacheName, Class<K> keyType, Class<V> valueType) {
        init(cacheName, keyType, valueType);
    }

    /**
     * Instantiates a new configuration class based on an existing Configuration.
     * @param cacheName the ScaleoutCache name
     * @param configuration the configuration the ScaleoutCacheConfiguration will use
     */
    ScaleoutCacheConfiguration(String cacheName, Configuration<K,V> configuration) {
        init(cacheName, configuration.getKeyType(), configuration.getValueType());
        if(configuration instanceof CompleteConfiguration) {
            initComplete((CompleteConfiguration<K,V>)configuration);
        }
    }

    /**
     * Instantiates a new configuration class based on an existing Configuration.
     * @param cacheName the ScaleoutCache name
     * @param configuration the configuration the ScaleoutCacheConfiguration will use
     */
    ScaleoutCacheConfiguration(String cacheName, CompleteConfiguration<K,V> configuration) {
        init(cacheName, configuration.getKeyType(), configuration.getValueType());
        initComplete(configuration);
    }

    private void init(String cacheName, Class<K> keyType, Class<V> valueType) {
        _cacheName  = cacheName;
        _keyType    = keyType;
        _valueType  = valueType;
    }

    private void initComplete(CompleteConfiguration<K,V> configuration) {
        _cacheEntryListenerConfigurations   = configuration.getCacheEntryListenerConfigurations();
        _expiryPolicyFactory                = configuration.getExpiryPolicyFactory();
        _cacheLoaderFactory                 = configuration.getCacheLoaderFactory();
        _cacheWriterFactory                 = configuration.getCacheWriterFactory();
        _isReadThrough                      = configuration.isReadThrough();
        _isWriteThrough                     = configuration.isWriteThrough();
    }

    @Override
    public Factory<ExpiryPolicy> getExpiryPolicyFactory() {
        return _expiryPolicyFactory;
    }

    @Override
    public boolean isReadThrough() {
        return _isReadThrough;
    }

    @Override
    public boolean isWriteThrough() {
        return _isWriteThrough;
    }

    @Override
    public boolean isStatisticsEnabled() {
        return false;
    }

    @Override
    public boolean isManagementEnabled() {
        return false;
    }

    @Override
    public Iterable<CacheEntryListenerConfiguration<K,V>> getCacheEntryListenerConfigurations() {
        if(_cacheEntryListenerConfigurations == null) {
            return Collections.emptyList();
        } else {
            return _cacheEntryListenerConfigurations;
        }
    }

    @Override
    public Factory<CacheLoader<K, V>> getCacheLoaderFactory() {
        return _cacheLoaderFactory;
    }

    @Override
    public Factory<CacheWriter<? super K, ? super V>> getCacheWriterFactory() {
        return _cacheWriterFactory;
    }

    /**
     * Returns the type of key used in key/value entries stored in a {@link ScaleoutCache}.
     * @return the type of the keys in key/value entries
     */
    @Override
    public Class<K> getKeyType() {
        return _keyType;
    }

    /**
     * Returns the type of value used in key/value entries stored in a {@link ScaleoutCache}.
     * @return the type of the values in key/value entries
     */
    @Override
    public Class<V> getValueType() {
        return _valueType;
    }

    /**
     * Returns if the {@link javax.cache.Cache} is configured as store by value or store by reference
     * @return false -- the objects are always stored inside the distributed NamedCache.
     */
    @Override
    public boolean isStoreByValue() {
        return false;
    }

    String getCacheName() {
        return _cacheName;
    }

    @Override
    public String toString() {
        return "ScaleoutCacheConfiguration{" +
                "_keyType=" + _keyType +
                ", _valueType=" + _valueType +
                ", _cacheName='" + _cacheName + '\'' +
                ", _cacheEntryListenerConfigurations=" + _cacheEntryListenerConfigurations +
                ", _expiryPolicyFactory=" + _expiryPolicyFactory +
                ", _cacheWriterFactory=" + _cacheWriterFactory +
                ", _cacheLoaderFactory=" + _cacheLoaderFactory +
                ", _isReadThrough=" + _isReadThrough +
                ", _isWriteThrough=" + _isWriteThrough +
                '}';
    }
}
