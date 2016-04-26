#!/bin/bash
CRAWL=2016-07
# TODO add root S3 bucket?
mvn clean
mvn package
echo "Copying built JAR to AWS S3"
aws s3 --profile cc-user \
    cp dkpro-c4corpus-hadoop/target/dkpro-c4corpus-hadoop-1.0.1-SNAPSHOT-standalone.jar \
    s3://tfmorris/c4corpus/
# Subnet subnet-f4037e83 us-east-1a subnet-28a18312 us-east-1d subnet-d9c972f2 us-east-1e
# 2016-07 crawl - 100 segments of 350 files each
# 100 file sample <crawlbase>/segments/*/warc/*-00001-*.warc.gz",
# 400 file sample (~1.1%) <crawlbase>/segments/*/warc/*-00[0-3]01-*.warc.gz",
# 1000 file sample <crawlbase>/segments/*/warc/*-000[0-9]0-*.warc.gz",
# 2% sample (700 files) <crawlbase>/segments/*/warc/*-0000[1-7]-*.warc.gz",
# ~10% sample (3600 files) <crawlbase>/segments/*/warc/*-00[0-3]0[1-9]-*.warc.gz",
# 4000 file sample <crawlbase>/segments/*/warc/*-00[0-3]0[0-9]-*.warc.gz",
echo "Creating cluster"
aws emr create-cluster \
    --name "C4Corpus phase 1 - 1.0.1-SNAPSHOT new-dataflow simhash2 - 2 x m4.4large + 12 x m4.4xlarge 1% sample $CRAWL" \
    --profile cc-user \
    --auto-terminate \
    --region us-east-1 \
    --applications Name=Hadoop Name=Ganglia \
    --ec2-attributes \
        '{"KeyName":"amazon-ec2-cc", 
        "InstanceProfile":"EMR_EC2_DefaultRole", 
        "SubnetId":"subnet-f4037e83",
        "EmrManagedMasterSecurityGroup":"sg-a2005dda",
        "EmrManagedSlaveSecurityGroup":"sg-a4005ddc"}' \
    --service-role EMR_DefaultRole \
    --enable-debugging \
    --release-label emr-4.4.0 \
    --log-uri 's3://tfmorris/logs' \
    --configuration \
    '[{
	"Classification": "hadoop-env",
	"Configurations": [
	    {
		"Classification": "export",
		"Configurations": [],
		"Properties": {
		    "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
		}
	    }
	],
	"Properties": {}
    }]' \
    --steps '[
        {"Args":["de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.full.Phase1FullJob",
        "-D","mapreduce.task.timeout=7200000",
        "-D", "mapreduce.map.failures.maxpercent=5",
        "-D", "mapreduce.map.maxattempts=2",
        "-D", "mapreduce.job.reduce.slowstart.completedmaps=0.95",
        "-D", "c4corpus.keepminimalhtml=true",
        "s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-'$CRAWL'/segments/*/warc/*-00[0-3]01-*.warc.gz",
        "s3://tfmorris/c4corpus/cc-phase1out-'$CRAWL'-400file-new-dataflow"],
        "Type":"CUSTOM_JAR",
        "ActionOnFailure":"TERMINATE_CLUSTER",
        "Jar":"s3://tfmorris/c4corpus/dkpro-c4corpus-hadoop-1.0.1-SNAPSHOT-standalone.jar",
        "Properties":"",
        "Name":"C4Corpus Phase 1 new"},
        {"Args":["de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.full.Phase2SortAndDedupe",
        "-D","mapreduce.task.timeout=7200000",
        "-D", "mapreduce.map.failures.maxpercent=5",
        "-D", "mapreduce.map.maxattempts=2",
        "-D", "mapreduce.job.reduce.slowstart.completedmaps=0.5",
        "-D", "mapreduce.job.reduces=50",
        "s3://tfmorris/c4corpus/cc-phase1out-'$CRAWL'-400file-new-dataflow/part*",
        "s3://tfmorris/c4corpus/cc-phase1out-'$CRAWL'-400file-new-dataflow/*/*.warc.gz",
        "s3://tfmorris/c4corpus/cc-phase2out-'$CRAWL'-400file-new-dataflow"],
        "Type":"CUSTOM_JAR",
        "ActionOnFailure":"TERMINATE_CLUSTER",
        "Jar":"s3://tfmorris/c4corpus/dkpro-c4corpus-hadoop-1.0.1-SNAPSHOT-standalone.jar",
        "Properties":"",
        "Name":"C4Corpus Phase 2 new"}
        ]' \
    --instance-groups '[
        {"InstanceCount":12,
            "BidPrice":"0.48",
            "InstanceGroupType":"TASK",
            "InstanceType":"m4.4xlarge",
            "Name":"Task - 12 x m4.4xlarge"},
        {"InstanceCount":2,
            "BidPrice":"0.48",
            "InstanceGroupType":"CORE",
            "InstanceType":"m4.4xlarge",
            "Name":"Core - 2 x m4.4xlarge"},
        {"InstanceCount":1,
            "InstanceGroupType":"MASTER",
            "InstanceType":"m3.xlarge",
            "Name":"Master - 1 x m3.xlarge"}]'

