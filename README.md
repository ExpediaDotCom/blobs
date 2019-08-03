[![Build Status](https://travis-ci.org/ExpediaDotCom/haystack-traces.svg?branch=master)](https://travis-ci.org/ExpediaDotCom/blobs)
[![License](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg)](https://github.com/ExpediaDotCom/haystack/blob/master/LICENSE)

## Blobs

This repository contains all the modules needed for an application. From creation of a blob, write it to a particular storage sink to read that blob back anytime you want. A blob can be any data that you may need to store somewhere for future need, preferably request and response data of a service. 

This library is divided into two for different types of usages: 

1. <strong>Standalone</strong>
	
    * [Example](https://github.com/ExpediaDotCom/blobs-example)
    * [Internals](stores/README.md)
    
2. <strong>Haystack</strong>

	* [Example](https://github.com/ExpediaDotCom/span-blob-example)
	* [Internals](haystack-blobs/README.md)
	
	For more information on haystack please refer [this](https://expediadotcom.github.io/haystack/).

## Table of content

- [Setup](#setup)
- [Blobs Core](#blobs-core)
- [Stores](#stores)

## Setup

##### Clone

Use the following command to clone the repository including the submodules present in it:

`git clone --recursive git@github.com:ExpediaDotCom/blobs.git`

##### Build

Use the following command to build the repository:

`mvn clean package`

## Blobs Core

This module contains all the core classes needed to instrument the creation of the blobs and then start the process of writing it to a [store](#stores). Only the Blob Model is created by the `blob.proto` present in [blobs-grpc-models](haystack-blobs/README.md#models) module inside the package `com.expedia.www.blobs.model`.

## Stores

Click [here](stores/README.md) for more details.