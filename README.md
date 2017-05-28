# Description
  
Big Lambda Serverless (BLS) is a framework for running MapReduce jobs on [AWS Lambda](https://aws.amazon.com/ru/lambda/)
powered by [Serverless](https://serverless.com/) framework.
BLS is based on some existing research about doing Big Data on AWS Lambda
like [this](https://aws.amazon.com/blogs/big-data/building-scalable-and-responsive-big-data-interfaces-with-aws-lambda/) and 
[this](https://aws.amazon.com/blogs/compute/ad-hoc-big-data-processing-made-simple-with-serverless-mapreduce/).


# Requirements

### tl;dr
Execute following command
```
./install_serverless_and_requirements.sh
```

### Description
For proper usage of Big Lambda Serverless you must install latest version of [Serverless](https://serverless.com/) framework
(well, at least 1.8.0)
```
npm install -g serverless
```
to install Serverless globally. You also need `npm` to be installed.

Serverless helps developers to build apps on AWS Lambda. It is really a great framework
supported by AWS.

Probably, next version will need additional 3rd parties libs, to install them execute
`
pip install -r requirements.txt -t vendored
`
or simply simply execute.
If you need additional 3rd parties libs for your implementation of Mapper or Reducer,
  just write package you need to `requirements.txt` and execute command above to 
  save this package for AWS Lambda.
  Nice tutorial about requirements file could be found [here](https://pip.pypa.io/en/stable/reference/pip_install/#requirements-file-format).


# Instructions

## Setup
To make BLS work, you need to fill up your credentials in `config.json` and `local.yml`.
Create `config.json` and `local.yml` in the root folder then 
carefully read `examples/config.json` and `examples/local.yml` for config's examples.
Generally, you need to fill
* Data and job bucket names and ARN's
* Lambda's names and params (etc. RAM and timeout)

The last one, you definitely want to create your own mapper and reducer for your tasks.

You can find all user's API is under `api` folder.
You need to write your own mapper and reducer classes.
Your implementation of these classes is inherited from `Base` class from `api/src/base.py`.

All internal logic is hidden under `Base` class. Your subclasses just needs to
1. Redefine global `output` buffer
2. Redefine `handler` function, to perform processing of the data
 
Probably sounds weired, so please check the examples and `example` folder.
Feel free to copy and paste from the examples.


## Running

To run a job on BLS you need to
1. Deploy Lambda to your AWS account using command `sls deploy`
2. Run `python run.py` from the root folder

Drink a cup of coffee and check your S3 job bucket for `result` 
file (yeap, it is named result).