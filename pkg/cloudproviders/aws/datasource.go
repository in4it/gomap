package aws

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/juju/loggo"
)

var (
	dataSourceLogger   = loggo.GetLogger("aws.dataSource")
	AWS_OWNER_ID       = "591542846629"
	CANONICAL_OWNER_ID = "099720109477"
)

type DataSource struct {
	svc  *ec2.EC2
	sess *session.Session
}

type Filter struct {
	Name  string
	Value string
}

func NewDataSource() *DataSource {
	sess := session.New()
	return &DataSource{
		sess: sess,
		svc:  ec2.New(sess),
	}
}

func (d *DataSource) getAMI(owner string, filter []Filter) (string, error) {
	var amiId string
	input := &ec2.DescribeImagesInput{
		Owners: []*string{aws.String(owner)},
	}
	ec2Filters := make([]*ec2.Filter, len(filter))
	for k, v := range filter {
		ec2Filters[k] = &ec2.Filter{
			Name:   aws.String(v.Name),
			Values: []*string{aws.String(v.Value)},
		}
	}
	input.SetFilters(ec2Filters)
	result, err := d.svc.DescribeImages(input)
	if err != nil {
		return "", err
	}
	if len(result.Images) == 0 {
		return amiId, fmt.Errorf("No ECS AMI found")
	}
	layout := "2006-01-02T15:04:05.000Z"
	var lastTime time.Time
	for _, v := range result.Images {
		t, err := time.Parse(layout, *v.CreationDate)
		if err != nil {
			return amiId, err
		}
		if t.After(lastTime) {
			lastTime = t
			amiId = aws.StringValue(v.ImageId)
		}
	}
	return amiId, nil

}
