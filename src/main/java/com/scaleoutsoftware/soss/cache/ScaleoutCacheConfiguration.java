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

import javax.cache.configuration.Configuration;

/**
 * {@link Configuration} implementation used to configure a {@link ScaleoutCache}.
 * @param <K> the type of the key in an entry stored in the {@link ScaleoutCache}
 * @param <V> the type of the value in an entry stored in the {@link ScaleoutCache}
 */
public class ScaleoutCacheConfiguration<K,V> implements Configuration<K,V> {

    private final Class<K> _keyType;
    private final Class<V> _valueType;
    private final String _cacheName;

    /**
     * Instantiates a new configuration for a {@link ScaleoutCache}.
     * @param cacheName the cache name
     * @param keyType the key type for key/value entries
     * @param valueType the value type for key/value entries
     */
    ScaleoutCacheConfiguration(String cacheName, Class<K> keyType, Class<V> valueType) {
        _cacheName = cacheName;
        _keyType = keyType;
        _valueType = valueType;
    }

    /**
     * Instantiates a new configuration class based on an existing Configuration.
     * @param cacheName the ScaleoutCache name
     * @param configuration the configuration the ScaleoutCacheConfiguration will use
     */
    ScaleoutCacheConfiguration(String cacheName, Configuration<K,V> configuration) {
        this(cacheName, configuration.getKeyType(), configuration.getValueType());
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

}
