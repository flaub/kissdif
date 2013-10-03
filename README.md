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

+ Response Headers

+ Status Codes

+ Request

```http
GET /env/table/by_name/Joe HTTP/1.1
Accept: application/json
Host: localhost:9090
```

+ Response 
```http
HTTP/1.1 200 OK
Content-Type: application/json
Etag: ""
```

### PUT `/{env}/{table}/_id/{id}`

### DELETE `/{env}/{table}/_id/{id}`
