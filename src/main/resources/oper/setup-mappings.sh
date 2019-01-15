#!/bin/bash
#curl -XPUT http://zhy.cauchy8389.com:9200/esh_complaints
curl -H "Content-Type: application/json;charset=UTF-8" -XPUT http://zhy.cauchy8389.com:9200/esh_complaints/ -d '
{
	"mappings":
	{
		 "complaints": 
		 {
		    "properties": 
		    {
			       "company": {
				  "type": "text",
				  "index": "false"
			       },
			       "companyResponse": {
				  "type": "text",
				  "index": "false"
			       },
			       "complaintId": {
				  "type": "keyword"
			       },
			       "consumerDisputed": {
				  "type": "boolean"
			       },
			       "dateReceived": {
				  "type": "date",
				  "format": "MM/dd/yyyy||MM/dd/yy"
			       },
			       "dateSent": {
				  "type": "date",
				  "format": "MM/dd/yyyy||MM/dd/yy"
			       },
			       "issueRaw": {
				  "type": "text",
				  "index": "false"
			       },
			       "issue": {
				  "type": "text"
			       },
			       "location": {
				  "type": "geo_point"
			       },
			       "product": {
				  "type": "text",
				  "index": "false",
				  "fields": {
				     "analyzed": {
					"type": "text"
				     }
				  }
			       },
			       "state": {
				  "type": "text",
				  "index": "false"
			       },
			       "subissue": {
				  "type": "text",
				  "index": "false",
				  "fields": {
				     "analyzed": {
					"type": "text"
				     }
				  }
			       },
			       "submittedVia": {
				  "type": "text",
				  "index": "false"
			       },
			       "subproduct": {
				  "type": "text",
				  "index": "false",
				  "fields": {
				     "analyzed": {
					"type": "text"
				     }
				  }
			       },
			       "timelyResponse": {
				  "type": "boolean"
			       },
			       "zip": {
				  "type": "keyword"
			       }
		    }
		 }
	 }
}'