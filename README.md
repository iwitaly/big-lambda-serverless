# Description
  
Big Lambda Serverless (BLS) is a framework for running MapReduce jobs on [AWS Lambda](https://aws.amazon.com/ru/lambda/)
powered by [Serverless](https://serverless.com/) framework.
BLS is based on some existing research about doing Big Data on AWS Lambda
like [this](https://aws.amazon.com/blogs/big-data/building-scalable-and-responsive-big-data-interfaces-with-aws-lambda/) and 
[this](https://aws.amazon.com/blogs/compute/ad-hoc-big-data-processing-made-simple-with-serverless-mapreduce/).


# Requirements
For proper usage of Big Lambda Serverless you must install latest version of [Serverless](https://serverless.com/) framework
(well, at least 1.8.0).

If you are to lazy to read, just do
```
npm install -g serverless
```
to install Serverless globally.
You also need `npm` to be installed.


Serverless helps developers to build apps on AWS Lambda. It is really a great framework
supported by AWS.

Probably, next version will need additional 3rd parties libs, to install them
```
pip install -r requirements.txt -t vendored
```

# Instructions

## Setup
I order to make BLS work, you need fill up your credentials in `config.json` and `local.yml`.
Create `config.json` and `local.yml` in root folder then 
carefully read `examples/config.json` and `examples/local.yml` for config's examples.
Generally, you need to fill
* data and job bucket names and ARN's
* Lambda's names and params (etc. RAM and timeout)

Feel free to copy and paste from examples.

The last one, you definitely want to create your own mapper and reducer for your tasks.
You need to modify `mapper.py` and `reducer.py` with your own logic. 

Check example of mapper and reducer functions at `example` folder.

## Running

All you need is to deploy Lambdas to your AWS account using command
 ```
 sls deploy
 ```
After that run `master` program at your computer `python master.py`.

You results will appears at you job bucket.
