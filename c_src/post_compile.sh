#!/bin/sh

rsync -uL --progress ${INSTANT_CLIENT_LIB_PATH}*.so* priv/
rm -f c_src/*.o c_src/*.d
