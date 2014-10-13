nsq-to-es
=========

NSQ to Elasticsearch transport utility.


Build Instructions
------------------

* export GOPATH=~/go
* mkdir -p ~/go/src
* cd ~/go/src
* git clone https://github.com/sethhall/nsq-to-es.git
* cd nsq-to-es
* go get
* cd ../github.com/bitly/nsq; git checkout v0.2.28; cd -
* cd ../github.com/bitly/go-nsq; git checkout v0.3.7; cd -
* go build
