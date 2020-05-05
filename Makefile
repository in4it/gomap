BINARY = wordcount
GOARCH = amd64

all: build-linux build-darwin

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY}-linux-${GOARCH} cmd/${BINARY}/main.go 

build-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY}-darwin-${GOARCH} cmd/${BINARY}/main.go 

clean:
	rm -f ${BINARY}-linux-${GOARCH}

