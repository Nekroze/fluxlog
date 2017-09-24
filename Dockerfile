FROM golang:1.9-alpine
RUN  apk add --no-cache git
WORKDIR /go/src/gitlab.com/nekroze/fluxlog/
RUN echo "Getting dependencies" \
  && GOOS=linux go get -u github.com/influxdata/influxdb/client/v2
COPY . .
RUN echo "Running build checks" \
  && [ -z "$(GOOS=linux go tool fix -diff .)" ] \
  && GOOS=linux go build -v
