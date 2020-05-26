package aws

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/juju/loggo"
)

var (
	spotInstanceLogger = loggo.GetLogger("aws.spotInstance")
)

type SpotInstanceConfig struct {
	launchSpecification *ec2.RequestSpotLaunchSpecification
	Input               string
	Executable          string
	Cmd                 string
	Region              string
	LogGroup            string
}

type SpotInstance struct {
	config               SpotInstanceConfig
	svc                  *ec2.EC2
	sess                 *session.Session
	SpotInstanceRequests map[string]*ec2.SpotInstanceRequest
}

func NewSpotInstance(config SpotInstanceConfig) *SpotInstance {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(config.Region)})
	if err != nil {
		readLogger.Errorf("Couldn't initialize aws session: %s", err)
		return nil
	}
	svc := ec2.New(sess)

	return &SpotInstance{
		sess:                 sess,
		svc:                  svc,
		config:               config,
		SpotInstanceRequests: make(map[string]*ec2.SpotInstanceRequest),
	}
}
func (s *SpotInstance) LaunchSpotInstance() (string, error) {
	input := &ec2.RequestSpotInstancesInput{
		LaunchSpecification: s.config.launchSpecification,
		InstanceCount:       aws.Int64(1),
	}

	fmt.Printf("Requesting spot instance (instance type: %s)\n", aws.StringValue(s.config.launchSpecification.InstanceType))

	result, err := s.svc.RequestSpotInstances(input)

	if err != nil {
		return "", err
	}

	if len(result.SpotInstanceRequests) == 0 {
		return "", fmt.Errorf("LaunchSpotInstance: no response in spot instance request")
	}

	return aws.StringValue(result.SpotInstanceRequests[0].SpotInstanceRequestId), nil
}
func (s *SpotInstance) SetLaunchSpecification(input []byte) error {
	if err := json.Unmarshal(input, &s.config.launchSpecification); err != nil {
		return err
	}

	fmt.Printf("Got launch config: %+v\n", s.config.launchSpecification)

	if s.config.launchSpecification.ImageId == nil {
		ds := NewDataSource(s.config.Region)
		amiID, err := ds.GetAMI(CANONICAL_OWNER_ID, []Filter{{Name: "name", Value: "ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-*"}})
		if err != nil {
			return fmt.Errorf("Error while fetching ami: %s", err)
		}
		s.config.launchSpecification.ImageId = aws.String(amiID)
	}

	fmt.Printf("Using AMI ID: %s (ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server)\n", aws.StringValue(s.config.launchSpecification.ImageId))

	// set userdata
	executableName := filepath.Base(s.config.Executable)
	userdata := `#!/bin/bash -e
				wget https://github.com/in4it/tee2cloudwatch/releases/download/0.0.3/tee2cloudwatch-linux-amd64
				chmod +x tee2cloudwatch-linux-amd64
				exec > >(./tee2cloudwatch-linux-amd64 -logGroup ` + s.config.LogGroup + ` -region ` + s.config.Region + `) 2>&1
				  sudo apt-get update -qq && sudo apt-get install awscli -y -qq
				  aws s3 cp ` + s.config.Executable + ` ` + executableName + `
				  chmod +x ` + executableName + `
				  ` + s.config.Cmd + `
				  echo "done! shutting down"
				  sudo shutdown now
				`
	userdataEnc := base64.StdEncoding.EncodeToString([]byte(userdata))

	s.config.launchSpecification.UserData = aws.String(userdataEnc)

	return nil
}
func (s *SpotInstance) GetSpotInstanceRequestStatus(requestId string) (string, string, error) {
	var i int
	for i = 1; i < 5; i++ {

		input := &ec2.DescribeSpotInstanceRequestsInput{
			SpotInstanceRequestIds: []*string{aws.String(requestId)},
		}

		result, err := s.svc.DescribeSpotInstanceRequests(input)

		if err != nil && !strings.HasPrefix(err.Error(), "InvalidSpotInstanceRequestID.NotFound") {
			return "", "", err
		}
		if len(result.SpotInstanceRequests) > 0 {
			s.SpotInstanceRequests[requestId] = result.SpotInstanceRequests[0]
			return aws.StringValue(result.SpotInstanceRequests[0].Status.Code), aws.StringValue(result.SpotInstanceRequests[0].Status.Message), nil
		}
		time.Sleep(time.Duration(i) * time.Second)
	}
	return "", "", fmt.Errorf("Couldn't get spot instance request status - no spot instances returned (tried %d times)", i)
}
func (s *SpotInstance) GetSpotInstanceRequestInstanceId(requestId string) string {
	return aws.StringValue(s.SpotInstanceRequests[requestId].InstanceId)
}
