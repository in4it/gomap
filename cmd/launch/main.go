package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/in4it/gomap/pkg/cloudproviders/aws"
)

func usage(err string) {
	fmt.Printf("Error: %s", err)
	flag.Usage()
	os.Exit(1)
}

func main() {
	var (
		launchSpecification string
		executable          string
		cmd                 string
		region              string
		logGroup            string
	)

	flag.StringVar(&launchSpecification, "launchSpecification", "", "launchSpecification")
	flag.StringVar(&executable, "executable", "", "executable")
	flag.StringVar(&cmd, "cmd", "", "cmd")
	flag.StringVar(&region, "region", "", "region")
	flag.StringVar(&logGroup, "logGroup", "", "logGroup")

	flag.Parse()

	if executable == "" {
		usage("executable not set")
	}
	if cmd == "" {
		usage("cmd not set")
	}
	if launchSpecification == "" {
		usage("launchSpecification not set")
	}
	if region == "" {
		usage("region not set")
	}
	if logGroup == "" {
		usage("specify a cloudwatch log group")
	}

	s := aws.NewSpotInstance(aws.SpotInstanceConfig{
		Executable: executable,
		Cmd:        cmd,
		Region:     region,
		LogGroup:   logGroup,
	})

	// read launchspec
	launchSpecificationJson, err := ioutil.ReadFile(launchSpecification)
	if err != nil {
		panic(err)
	}

	err = s.SetLaunchSpecification(launchSpecificationJson)
	if err != nil {
		panic(err)
	}

	s.SetLaunchSpecificationUserdata(getUserdata(executable, logGroup, region, cmd))

	spotInstanceRequestId, err := s.LaunchSpotInstance()

	if err != nil {
		panic(err)
	}

	secondsWaited := 0
	waitInterval := 5
	for {
		status, _, err := s.GetSpotInstanceRequestStatus(spotInstanceRequestId)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			os.Exit(1)
		}
		if status == "fulfilled" {
			fmt.Printf("SpotInstance Request is fulfilled")
			break
		}
		fmt.Printf("Waiting for spotinstance to be fulfilled. Current status '%s' [%d seconds elapsed]\n", status, secondsWaited)
		secondsWaited += waitInterval
		time.Sleep(time.Duration(waitInterval) * time.Second)
	}

	instanceId := s.GetSpotInstanceRequestInstanceId(spotInstanceRequestId)
	fmt.Printf("Instance launched: %s\n", instanceId)
	fmt.Printf("See cloudwatch logs for details\n")

	return
}

func getUserdata(executable, logGroup, region, cmd string) string {
	// set userdata
	executableName := filepath.Base(executable)
	userdata := `#!/bin/bash
		wget -q https://github.com/in4it/tee2cloudwatch/releases/download/0.0.3/tee2cloudwatch-linux-amd64
		wget -q https://github.com/in4it/gomap/releases/download/0.0.1-rc1/launch-agent-linux-amd64
		chmod +x tee2cloudwatch-linux-amd64 launch-agent-linux-amd64
		exec > >(./tee2cloudwatch-linux-amd64 -logGroup ` + logGroup + ` -region ` + region + `) 2>&1
			./launch-agent-linux-amd64 -s3get ` + executable + `
			chmod +x ` + executableName + `
			` + cmd + `
			echo "done! shutting down"
			sudo shutdown now
				`
	return userdata
}
