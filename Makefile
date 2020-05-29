BINARY1 = wordcount
BINARY2 = launch
BINARY3 = launch-agent
GOARCH = amd64

all: build-linux build-darwin

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY1}-linux-${GOARCH} cmd/${BINARY1}/main.go 
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY2}-linux-${GOARCH} cmd/${BINARY2}/main.go 
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY3}-linux-${GOARCH} cmd/${BINARY3}/main.go 

build-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY1}-darwin-${GOARCH} cmd/${BINARY1}/main.go 
	CGO_ENABLED=0 GOOS=darwin GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY2}-darwin-${GOARCH} cmd/${BINARY2}/main.go 
	CGO_ENABLED=0 GOOS=darwin GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY3}-darwin-${GOARCH} cmd/${BINARY3}/main.go 

clean:
	rm -f ${BINARY}-linux-${GOARCH}

