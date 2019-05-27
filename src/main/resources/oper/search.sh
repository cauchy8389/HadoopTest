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

curl -H "Content-Type: application/json;charset=UTF-8" -XGET http://zhy.cauchy8389.com:9200/esh_complaints/_search?pretty -d '
{
  "query" : {
    "multi_match": {
            "query" : "11935",
            "fields" : ["complaintId", "zip"]
     }
  }
}'