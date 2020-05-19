package aws

import (
	"encoding/json"
	"fmt"

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
	Region              string
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

	return &SpotInstance{sess: sess, svc: svc, SpotInstanceRequests: make(map[string]*ec2.SpotInstanceRequest)}
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
	// ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server
	fmt.Printf("Got launch config: %+v\n", s.config.launchSpecification)
	return nil
}
func (s *SpotInstance) GetSpotInstanceRequestStatus(requestId string) (string, string, error) {
	input := &ec2.DescribeSpotInstanceRequestsInput{
		SpotInstanceRequestIds: []*string{aws.String(requestId)},
	}

	result, err := s.svc.DescribeSpotInstanceRequests(input)

	if err != nil {
		return "", "", err
	}
	if len(result.SpotInstanceRequests) == 0 {
		return "", "", fmt.Errorf("No spot instance requests returned")
	}
	s.SpotInstanceRequests[requestId] = result.SpotInstanceRequests[0]

	return aws.StringValue(result.SpotInstanceRequests[0].Status.Code), aws.StringValue(result.SpotInstanceRequests[0].Status.Message), nil
}
func (s *SpotInstance) GetSpotInstanceRequestInstanceId(requestId string) string {
	return aws.StringValue(s.SpotInstanceRequests[requestId].InstanceId)
}
