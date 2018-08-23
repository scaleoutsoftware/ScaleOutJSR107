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

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * A {@link CachingProvider} implementation that provides {@link ScaleoutCacheManager}s.
 */
public class ScaleoutCachingProvider implements CachingProvider {

    private Properties _defaultProperties;
    private URI _defaultUri;
    private ClassLoader _defaultClassLoader;
    private List<ScaleoutCacheManager> _managers;

    /**
     * Instantiates a ScaleoutCachingProvider.
     */
    ScaleoutCachingProvider() {
        _defaultClassLoader = ClassLoader.getSystemClassLoader();
        try {
            _defaultUri = new URI("soss/cache/api");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        _defaultProperties = new Properties();
        _defaultProperties.put("namespace", "default");
        _managers = new LinkedList<>();
    }

    /**
     * Returns the default classloader.
     * @return the default classloader
     */
    @Override
    public ClassLoader getDefaultClassLoader() {
        return _defaultClassLoader;
    }

    /**
     * Returns the default URI of this caching provider.
     * @return the default URI of this caching provider
     */
    @Override
    public URI getDefaultURI() {
        return _defaultUri;
    }

    /**
     * Returns the default properties of this caching provider.
     * @return the default properties of this caching provider
     */
    @Override
    public Properties getDefaultProperties() {
        return _defaultProperties;

    }

    /**
     * Returns a ScaleoutCacheManager.
     * @param uri the URI for a {@link CacheManager}
     * @param classLoader the classloader for a {@link CacheManager}
     * @param properties the properties for a {@link CacheManager}
     * @return a {@link ScaleoutCacheManager}
     */
    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        ScaleoutCacheManager manager = new ScaleoutCacheManager(this, uri, properties, classLoader);
        _managers.add(manager);
        return manager;
    }

    /**
     * Returns a ScaleoutCacheManager.
     * @param uri the URI for a {@link CacheManager}
     * @param classLoader the classloader for a {@link CacheManager}
     * @return a {@link ScaleoutCacheManager}
     */
    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        ScaleoutCacheManager manager = new ScaleoutCacheManager(this, uri, _defaultProperties, classLoader);
        _managers.add(manager);
        return manager;
    }

    /**
     * Returns a ScaleoutCacheManager.
     * @return a {@link ScaleoutCacheManager}
     */
    @Override
    public CacheManager getCacheManager() {
        ScaleoutCacheManager manager = new ScaleoutCacheManager(this, _defaultUri, _defaultProperties, _defaultClassLoader);
        _managers.add(manager);
        return manager;
    }

    /**
     * Closes this {@link CachingProvider}.
     */
    @Override
    public void close() {
        close(null, null);
    }

    /**
     * Closes this {@link CachingProvider}.
     * @param classLoader to class loader close
     */
    @Override
    public void close(ClassLoader classLoader) {
        close(null, classLoader );
    }

    /**
     * Closes this caching provider and all CacheManagers created with the URI and classLoader.
     * @param uri the URI to release
     * @param classLoader the classloader to close
     */
    @Override
    public void close(URI uri, ClassLoader classLoader) {
        for(ScaleoutCacheManager manager : _managers) {
            boolean close = false;
            if(uri != null) {
                close = uri.equals(manager.getURI());
                if(!close) {
                    continue;
                }
            }
            if(classLoader != null) {
                close |= classLoader.equals(manager.getClassLoader());
            }
            if(uri == null && classLoader == null) {
                close = true;
            }

            if(close) {
                manager.close();
                _managers.remove(manager);
            }
        }
    }

    /**
     * Determines whether an optional feature is supported by the {@link CachingProvider}.
     * @param optionalFeature the optional feature
     * @return true/false if the optional feature is supported
     */
    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }
}
