package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/in4it/gomap/pkg/cloudproviders/aws"
)

func main() {
	var (
		launchSpecification string
		input               string
		executable          string
		region              string
	)

	flag.StringVar(&launchSpecification, "launchSpecification", "", "launchSpecification")
	flag.StringVar(&input, "input", "", "input")
	flag.StringVar(&executable, "source", "", "source")
	flag.StringVar(&region, "region", "", "region")

	flag.Parse()

	s := aws.NewSpotInstance(aws.SpotInstanceConfig{
		Input:      input,
		Executable: executable,
		Region:     region,
	})

	// read launchspec
	launchSpecificationJson, err := ioutil.ReadFile(launchSpecification)
	if err != nil {
		panic(err)
	}

	s.SetLaunchSpecification(launchSpecificationJson)

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

	return
}
