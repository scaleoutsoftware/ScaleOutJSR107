# JSR107 specification implemented using ScaleOut StateServer

## Introduction

The ScaleOut Cache API for Java allows developers to store Java objects inside ScaleOut StateServer using the JSR107 caching paradigm. The API implements the CachingProvider, CachingManager, and Cache interfaces. 

Our **[docs](https://scaleoutsoftware.github.io/ScaleOutJSR107/)** will get you up and running in no time.

To use the library in your project:

### Gradle

For Gradle, you can add the ScaleOut API Repository to your build.gradle by adding the following under repositories: 

``` 
repositories {
    mavenCentral()
    maven {
        url "https://repo.scaleoutsoftware.com/repository/external"
    }
}
```

...and then you can add the ScaleOut JSR107 API as a dependency:

```
compile group: 'com.scaleoutsoftware.client', name: "jsr107", version: '1.2'
```

### Maven

For Maven, you can add the ScaleOut API Repository to your pom.xml by adding the following repository reference: 

```
<repository>
    <id>ScaleOut API Repository</id>
    <url>https://repo.scaleoutsoftware.com/repository/external</url>
</repository>
```

...and then you can add the ScaleOut Spring JSR107 API as a dependency:

```
<dependencies>
	<dependency>
	  <groupId>com.scaleoutsoftware.client</groupId>
	  <artifactId>jsr107</artifactId>
	  <version>1.2</version>
	</dependency>
</dependencies>
``` 

This library is open source and has dependencies on other ScaleOut Software products. 

License: Apache 2 