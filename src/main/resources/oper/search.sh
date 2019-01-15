curl -H "Content-Type: application/json;charset=UTF-8" -XGET http://zhy.cauchy8389.com:9200/esh_complaints/_search?pretty -d '
{
  "query" : {
    "term" : {
      "complaintId" : {
        "value": 1188061
      }
    }
  }
}'