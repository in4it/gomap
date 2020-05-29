# gomap
[![Travis Status for in4it/gomap](https://travis-ci.org/in4it/gomap.svg?branch=master)](https://travis-ci.org/in4it/gomap)
[![godoc for in4it/gomap](https://godoc.org/github.com/in4it/gomap?status.svg)](http://godoc.org/github.com/in4it/gomap)

Run your MapReduce workloads as a single binary on a single machine with multiple CPUs and high memory. Pricing of a lot of small machines vs heavy machines is the same on most cloud providers.

# Run gomap on AWS
You can run gomap on AWS on a spot instance using the launcher.

## Configuration

Example launch specification (if the AMI is not supplied, it'll launch the latest ubuntu bionic AMI):
```
{
    "IamInstanceProfile": {
      "Arn": "arn:aws:iam::1234567890:instance-profile/gomap"
    },
    "InstanceType": "r4.large",
    "NetworkInterfaces": [
      {
        "DeviceIndex": 0,
        "Groups": ["sg-0123456789"],
        "SubnetId": "subnet-01234567890"
      }
    ]  
}
```

Note: the instance profile should have s3 & cloudwatch logs access

## Run

Download the wordcount and launch binary from the release page, and run:
```
aws s3 cp wordcount-linux-amd64 s3://yourbucket/binaries/wordcount
./launch -launchSpecification launchspec.json -region eu-west-1 -cmd "./wordcount -input s3://yourbucket/inputfile.txt" -executable s3://yourbucket/binaries/wordcount
```