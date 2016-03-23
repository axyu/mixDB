#!/bin/bash
export BOOST_CFLAGS='-I../boost-x86/'
export BOOST_LIBS='-L../boost-x86/stage/lib/ -lboost_system -lboost_timer'

export GFLAGS_CFLAGS='-I../gflags-x86/build/include/'
export GFLAGS_LIBS='-L../gflags-x86/build/lib/ -lgflags'

export GLOG_CFLAGS='-I../glog-x86/src/'
export GLOG_LIBS='-L../glog-x86/.libs/ -lglog'

export PROTO_CFLAGS='-I../protobuf-x86/src/'
export PROTO_LIBS='-L../protobuf-x86/src/.libs/ -lprotobuf'

export RE2_CFLAGS='-I../re2-x86/'
export RE2_LIBS='-L../re2-x86/.obj/so/ -lre2'
