FROM ubuntu:xenial

RUN apt-get update && apt-get -y install \
	luarocks librados-dev build-essential \
	autoconf libtool

COPY . /code
WORKDIR /code

CMD ./bootstrap.sh && ./configure && make
