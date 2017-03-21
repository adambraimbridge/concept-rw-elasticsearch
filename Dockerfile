FROM alpine:3.4

COPY . .git /concept-rw-elasticsearch/

RUN apk add --update bash \
  && apk --update add git bzr go ca-certificates \
  && export GOPATH=/gopath \
  && REPO_PATH="github.com/Financial-Times/concept-rw-elasticsearch" \
  && mkdir -p $GOPATH/src/${REPO_PATH} \
  && mv concept-rw-elasticsearch/* $GOPATH/src/${REPO_PATH} \
  && rm -r concept-rw-elasticsearch \
  && cd $GOPATH/src/${REPO_PATH} \
  && go get -u github.com/kardianos/govendor \
  && $GOPATH/bin/govendor sync \
  && go get -t ./... \
  && go build \
  && mv concept-rw-elasticsearch /concept-rw-elasticsearch \
  && apk del go git bzr \
  && rm -rf $GOPATH /var/cache/apk/*

CMD [ "/concept-rw-elasticsearch" ]