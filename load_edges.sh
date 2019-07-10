curl -X POST -H 'Content-Type: application/json' \
    https://grc-cluster.cluster-csyluf7pal0m.ap-northeast-1.neptune.amazonaws.com:8182/loader -d '
    {
      "source" : "s3://load-data-to-neptune/data/order.csv",
      "format" : "csv",
      "iamRoleArn" : "arn:aws:iam::849963555403:role/NeptuneLoadFromS3",
      "region" : "ap-northeast-1",
      "failOnError" : "FALSE",
      "parallelism" : "HIGH"
    }'
