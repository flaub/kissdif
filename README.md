kissdif
=======

KISS Data InterFace

# REST API

## Environment Resources

## Table Resources

## Document Resources

### GET `/{env}/{table}/{index}/{key}`
Retrieve a document.

+ Parameters

	+ **env** - Environment name
	+ **table** - Table name
	+ **index** - Index to perform lookup on
	+ **key** - Value of the key used in a lookup

+ Request Headers

	+ If-None-Match - Double quoted document's revision token

+ Response Headers

	+ ETag - Double quoted document's revision token.

+ Status Codes

	+ 200 OK - Request completed successfully
	+ 304 Not Modified - Document wasn't modified since specified revision
	+ 400 Bad Request - The format of the request of revision was invalid
	+ 404 Not Found - Document not found

+ Request

```http
GET /env/table/_id/1 HTTP/1.1
Accept: application/json
Host: localhost:9900
```

+ Response 

```http
HTTP/1.1 200 OK
Content-Type: application/json
Etag: ""
```

### PUT `/{env}/{table}/_id/{id}`

The PUT method creates a new named document, or creates a new revision of the existing document.

+ Parameters

	+ **env** - Environment name
	+ **table** - Table name
	+ **id** - Document ID

+ Request Headers

	+ If-Match - Document's revision

+ Response Headers

	+ ETag - Double quoted document's revision token.

### DELETE `/{env}/{table}/_id/{id}`
