Introduction
============

In this documentation, the basics of the KIT Data Manager RESTful API of
the MetaStore Service are described. You will be guided through the
first steps of register an XML schema, update it. After that add an
appropriate metadata document to MetaStore which may be linked to a data
resource.

This documentation assumes, that you have an instance of the KIT Data
Manager MetaStore repository service installed locally. If the
repository is running on another host or port you should change hostname
and/or port accordingly. Furthermore, the examples assume that you are
using the repository without authentication and authorization, which is
provided by another service. If you plan to use this optional service,
please refer to its documentation first to see how the examples in this
documentation have to be modified in order to work with authentication.
Typically, this should be achieved by simple adding an additional header
entry.

The example structure is identical for all examples below. Each example
starts with a CURL command that can be run by copy&paste to your
console/terminal window. The second part shows the HTTP request sent to
the server including arguments and required headers. Finally, the third
block shows the response comming from the server. In between, special
characteristics of the calls are explained together with additional,
optional arguments or alternative responses.

> **Note**
>
> For technical reasons, all metadata resources shown in the examples
> contain all fields, e.g. also empty lists or fields with value 'null'.
> You may ignore most of them as long as they are not needed. Some of
> them will be assigned by the server, others remain empty or null as
> long as you don’t assign any value to them. All fields mandatory at
> creation time are explained in the resource creation example.

XML (Schema)
============

Schema Registration and Management
----------------------------------

In this first section, the handling of schema resources is explained. It
all starts with creating your first xml schema resource. The model of a
metadata schema record looks like this:

    {
      "schemaId" : "...",
      "schemaVersion" : 1,
      "mimeType" : "...",
      "type" : "...",
      "createdAt" : "...",
      "lastUpdate" : "...",
      "acl" : [ {
        "id" : 1,
        "sid" : "...",
        "permission" : "..."
      } ],
      "schemaDocumentUri" : "...",
      "schemaHash" : "...",
      "locked" : false
    }

At least the following elements are expected to be provided by the user:

-   schemaId: A unique label for the schema.

-   mimeType: The resource type must be assigned by the user. For XSD
    schemas this should be *application/xml*

-   type: XML or JSON. For XSD schemas this should be *XML*

In addition, ACL may be useful to make schema editable by others. (This
will be of interest while updating an existing schema)

Registering a Metadata Schema Document
--------------------------------------

The following example shows the creation of the first xsd schema only
providing mandatory fields mentioned above:

    schema-record.json:
    {
      "schemaId" : "my_first_xsd",
      "mimeType" : "application/xml",
      "type" : "XML"
    }

    schema.xsd:
    <xs:schema targetNamespace="http://www.example.org/schema/xsd/"
                xmlns="http://www.example.org/schema/xsd/"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                elementFormDefault="qualified" attributeFormDefault="unqualified">

      <xs:element name="metadata">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="title" type="xs:string"/>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>

    $ curl 'http://localhost:8080/api/v1/schemas' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'schema=@schema.xsd;type=application/xml' \
        -F 'record=@schema-record.json;type=application/json'

You can see, that most of the sent metadata schema record is empty. Only
schemaId, mimeType and type are provided by the user. HTTP-wise the call
looks as follows:

    POST /api/v1/schemas HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=schema; filename=schema.xsd
    Content-Type: application/xml

    <xs:schema targetNamespace="http://www.example.org/schema/xsd/"
            xmlns="http://www.example.org/schema/xsd/"
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified" attributeFormDefault="unqualified">

    <xs:element name="metadata">
      <xs:complexType>
        <xs:sequence>
          <xs:element name="title" type="xs:string"/>
        </xs:sequence>
      </xs:complexType>
    </xs:element>

    </xs:schema>
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=schema-record.json
    Content-Type: application/json

    {"schemaId":"my_first_xsd","pid":null,"schemaVersion":null,"label":null,"definition":null,"comment":null,"mimeType":"application/xml","type":"XML","createdAt":null,"lastUpdate":null,"acl":[],"schemaDocumentUri":null,"schemaHash":null,"locked":false}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As Content-Type only 'application/json' is supported and should be
provided. The other headers are typically set by the HTTP client. After
validating the provided document, adding missing information where
possible and persisting the created resource, the result is sent back to
the user and will look that way:

    HTTP/1.1 201 Created
    Location: http://localhost:8080/api/v1/schemas/my_first_xsd?version=1
    ETag: "-887652601"
    Content-Type: application/json
    Content-Length: 389

    {
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 1,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:32Z",
      "lastUpdate" : "2021-06-22T14:37:32.151Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_xsd?version=1",
      "locked" : false
    }

What you see is, that the metadata schema record looks different from
the original document. All remaining elements received a value by the
server. Furthermore, you’ll find an ETag header with the current ETag of
the resource. This value is returned by POST, GET and PUT calls and must
be provided for all calls modifying the resource, e.g. POST, PUT and
DELETE, in order to avoid conflicts.

### Getting a Metadata Schema Record

For obtaining one metadata schema record you have to provide the value
of the field 'schemaId'.

> **Note**
>
> As 'Accept' field you have to provide
> 'application/vnd.datamanager.schema-record+json' otherwise you will
> get the metadata schema instead.

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_xsd' -i -X GET \
        -H 'Accept: application/vnd.datamanager.schema-record+json'

In the actual HTTP request just access the path of the resource using
the base path and the 'schemaid'. Be aware that you also have to provide
the 'Accept' field.

    GET /api/v1/schemas/my_first_xsd HTTP/1.1
    Accept: application/vnd.datamanager.schema-record+json
    Host: localhost:8080

As a result, you receive the metadata schema record send before and
again the corresponding ETag in the HTTP response header.

    HTTP/1.1 200 OK
    ETag: "-887652601"
    Content-Type: application/vnd.datamanager.schema-record+json
    Content-Length: 389

    {
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 1,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:32Z",
      "lastUpdate" : "2021-06-22T14:37:32.151Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_xsd?version=1",
      "locked" : false
    }

### Getting a Metadata Schema Document

For obtaining accessible metadata schemas you also have to provide the
'schemaId'. For accessing schema document you don’t have to provide the
'Accept' header.

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_xsd' -i -X GET

In the actual HTTP request there is nothing special. You just access the
path of the resource using the base path and the 'schemaId'.

    GET /api/v1/schemas/my_first_xsd HTTP/1.1
    Host: localhost:8080

As a result, you receive the XSD schema send before.

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Content-Length: 591
    Accept-Ranges: bytes

    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema targetNamespace="http://www.example.org/schema/xsd/" xmlns="http://www.example.org/schema/xsd/" xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" attributeFormDefault="unqualified">

        <xs:element name="metadata">

            <xs:complexType>

                <xs:sequence>

                    <xs:element name="title" type="xs:string"/>

                </xs:sequence>

            </xs:complexType>

        </xs:element>

    </xs:schema>

### Updating a Metadata Schema Document (add mandatory 'date' field)

> **Note**
>
> Updating a metadata schema document will not break old metadata
> documents. As every update results in a new version 'old' metadata
> schema documents are still available.

For updating an existing metadata schema (record) a valid ETag is
needed. The actual ETag is available via the HTTP GET call of the
metadata schema record. (see above) Just send an HTTP POST with the
updated metadata schema document and/or metadata schema record.

    schema-v2.xsd:
    <xs:schema targetNamespace="http://www.example.org/schema/xsd/"
                xmlns="http://www.example.org/schema/xsd/"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                elementFormDefault="qualified" attributeFormDefault="unqualified">

      <xs:element name="metadata">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="title" type="xs:string"/>
            <xs:element name="date" type="xs:date"/>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_xsd' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "-887652601"' \
        -F 'schema=@schema-v2.xsd;type=application/xml'

HTTP-wise the call looks as follows:

    PUT /api/v1/schemas/my_first_xsd HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "-887652601"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=schema; filename=schema-v2.xsd
    Content-Type: application/xml

    <xs:schema targetNamespace="http://www.example.org/schema/xsd/"
            xmlns="http://www.example.org/schema/xsd/"
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified" attributeFormDefault="unqualified">

    <xs:element name="metadata">
      <xs:complexType>
        <xs:sequence>
          <xs:element name="title" type="xs:string"/>
          <xs:element name="date" type="xs:date"/>
        </xs:sequence>
      </xs:complexType>
    </xs:element>

    </xs:schema>
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As a result, you receive the updated schema record and in the HTTP
response header the new location URL and the ETag.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/schemas/my_first_xsd?version=2
    ETag: "1833460449"
    Content-Type: application/json
    Content-Length: 389

    {
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 2,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:32Z",
      "lastUpdate" : "2021-06-22T14:37:33.339Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_xsd?version=2",
      "locked" : false
    }

### Updating a Metadata Schema Document (add optional 'note' field)

For updating existing metadata schema document we have to provide the
new ETag. Just send an HTTP POST with the updated metadata schema
document and/or metadata schema record.

    schema-v3.xsd:
    <xs:schema targetNamespace="http://www.example.org/schema/xsd/"
                xmlns="http://www.example.org/schema/xsd/"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                elementFormDefault="qualified" attributeFormDefault="unqualified">

      <xs:element name="metadata">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="title" type="xs:string"/>
            <xs:element name="date" type="xs:date"/>
            <xs:element name="note" type="xs:string" minOccurs="0"/>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_xsd' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "1833460449"' \
        -F 'schema=@schema-v3.xsd;type=application/xml'

HTTP-wise the call looks as follows:

    PUT /api/v1/schemas/my_first_xsd HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "1833460449"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=schema; filename=schema-v3.xsd
    Content-Type: application/xml

    <xs:schema targetNamespace="http://www.example.org/schema/xsd/"
            xmlns="http://www.example.org/schema/xsd/"
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified" attributeFormDefault="unqualified">

    <xs:element name="metadata">
      <xs:complexType>
        <xs:sequence>
          <xs:element name="title" type="xs:string"/>
          <xs:element name="date" type="xs:date"/>
          <xs:element name="note" type="xs:string" minOccurs="0"/>
        </xs:sequence>
      </xs:complexType>
    </xs:element>

    </xs:schema>
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As a result, you receive the updated schema record and in the HTTP
response header the new location URL and the ETag.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/schemas/my_first_xsd?version=3
    ETag: "-1377314606"
    Content-Type: application/json
    Content-Length: 389

    {
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 3,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:32Z",
      "lastUpdate" : "2021-06-22T14:37:33.594Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_xsd?version=3",
      "locked" : false
    }

The updated schema record contains three modified fields:
'schemaVersion', 'lastUpdate' and 'schemaDocumentUri'.

Registering another Metadata Schema Document
--------------------------------------------

The following example shows the creation of another xsd schema only
providing mandatory fields mentioned above:

    another-schema-record.json:
    {
      "schemaId" : "another_xsd",
      "mimeType" : "application/xml",
      "type" : "XML"
    }

    another-schema.xsd:
    <xs:schema targetNamespace="http://www.example.org/schema/xsd/example"
            xmlns="http://www.example.org/schema/xsd/example"
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified" attributeFormDefault="unqualified">
      <xs:element name="metadata">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="description" type="xs:string"/>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>

    $ curl 'http://localhost:8080/api/v1/schemas' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'schema=@another-schema.xsd;type=application/xml' \
        -F 'record=@another-schema-record.json;type=application/json'

HTTP-wise the call looks as follows:

    POST /api/v1/schemas HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=schema; filename=another-schema.xsd
    Content-Type: application/xml

    <xs:schema targetNamespace="http://www.example.org/schema/xsd/example"
            xmlns="http://www.example.org/schema/xsd/example"
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified" attributeFormDefault="unqualified">

    <xs:element name="metadata">
      <xs:complexType>
        <xs:sequence>
          <xs:element name="description" type="xs:string"/>
        </xs:sequence>
      </xs:complexType>
    </xs:element>

    </xs:schema>
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=another-schema-record.json
    Content-Type: application/json

    {"schemaId":"another_xsd","pid":null,"schemaVersion":null,"label":null,"definition":null,"comment":null,"mimeType":"application/xml","type":"XML","createdAt":null,"lastUpdate":null,"acl":[],"schemaDocumentUri":null,"schemaHash":null,"locked":false}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As Content-Type only 'application/json' is supported and should be
provided. The other headers are typically set by the HTTP client. After
validating the provided document, adding missing information where
possible and persisting the created resource, the result is sent back to
the user and will look that way:

    HTTP/1.1 201 Created
    Location: http://localhost:8080/api/v1/schemas/another_xsd?version=1
    ETag: "844243362"
    Content-Type: application/json
    Content-Length: 387

    {
      "schemaId" : "another_xsd",
      "schemaVersion" : 1,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:33Z",
      "lastUpdate" : "2021-06-22T14:37:33.759Z",
      "acl" : [ {
        "id" : 2,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/another_xsd?version=1",
      "locked" : false
    }

Now there are two schemaIds registered in the metadata schema registry.

### Getting a List of Metadata Schema Records

Obtaining all accessible metadata schema records.

    $ curl 'http://localhost:8080/api/v1/schemas' -i -X GET

Same for HTTP request:

    GET /api/v1/schemas HTTP/1.1
    Host: localhost:8080

As a result, you receive a list of metadata schema records.

    HTTP/1.1 200 OK
    Content-Range: 0-19/2
    Content-Type: application/json
    Content-Length: 782

    [ {
      "schemaId" : "another_xsd",
      "schemaVersion" : 1,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:33Z",
      "lastUpdate" : "2021-06-22T14:37:33.759Z",
      "acl" : [ {
        "id" : 2,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/another_xsd?version=1",
      "locked" : false
    }, {
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 3,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:32Z",
      "lastUpdate" : "2021-06-22T14:37:33.594Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_xsd?version=3",
      "locked" : false
    } ]

> **Note**
>
> Only the current version of each schemaId is listed.

> **Note**
>
> The header contains the field 'Content-Range" which displays delivered
> indices and the maximum number of available schema records. If there
> are more than 20 schemas registered you have to provide page and/or
> size as additional query parameters.

-   page: Number of the page you want to get **(starting with page 0)**

-   size: Number of entries per page.

The modified HTTP request with pagination looks like follows:

    GET /api/v1/schemas?page=0&size=20 HTTP/1.1
    Host: localhost:8080

### Getting a List of all Schema Records for a Specific SchemaId

If you want to obtain all versions of a specific schema you may add the
schemaId as a filter parameter. This may look like this:

    $ curl 'http://localhost:8080/api/v1/schemas?schemaId=my_first_xsd' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/schemas?schemaId=my_first_xsd HTTP/1.1
    Host: localhost:8080

As a result, you receive a list of metadata schema records in descending
order. (current version first)

    HTTP/1.1 200 OK
    Content-Range: 0-19/3
    Content-Type: application/json
    Content-Length: 1175

    [ {
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 3,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:32Z",
      "lastUpdate" : "2021-06-22T14:37:33.594Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_xsd?version=3",
      "locked" : false
    }, {
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 2,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:32Z",
      "lastUpdate" : "2021-06-22T14:37:33.339Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_xsd?version=2",
      "locked" : false
    }, {
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 1,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:32Z",
      "lastUpdate" : "2021-06-22T14:37:32.151Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_xsd?version=1",
      "locked" : false
    } ]

### Getting current Version of Metadata Schema Document

To get the current version of the metadata schema document just send an
HTTP GET with the linked 'schemaId':

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_xsd' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/schemas/my_first_xsd HTTP/1.1
    Host: localhost:8080

As a result, you receive the XSD schema document sent before.

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Content-Length: 767
    Accept-Ranges: bytes

    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema targetNamespace="http://www.example.org/schema/xsd/" xmlns="http://www.example.org/schema/xsd/" xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" attributeFormDefault="unqualified">

        <xs:element name="metadata">

            <xs:complexType>

                <xs:sequence>

                    <xs:element name="title" type="xs:string"/>

                    <xs:element name="date" type="xs:date"/>

                    <xs:element name="note" type="xs:string" minOccurs="0"/>

                </xs:sequence>

            </xs:complexType>

        </xs:element>

    </xs:schema>

### Getting a specific Version of Metadata Schema Document

To get a specific version of the metadata schema document just send an
HTTP GET with the linked 'schemaId' and the version number you are
looking for as query parameter:

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_xsd?version=1' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/schemas/my_first_xsd?version=1 HTTP/1.1
    Host: localhost:8080

As a result, you receive the initial XSD schema document (version 1).

    HTTP/1.1 200 OK
    Content-Type: application/xml
    Content-Length: 591
    Accept-Ranges: bytes

    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema targetNamespace="http://www.example.org/schema/xsd/" xmlns="http://www.example.org/schema/xsd/" xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" attributeFormDefault="unqualified">

        <xs:element name="metadata">

            <xs:complexType>

                <xs:sequence>

                    <xs:element name="title" type="xs:string"/>

                </xs:sequence>

            </xs:complexType>

        </xs:element>

    </xs:schema>

### Validating Metadata Document

Before an ingest of metadata is made the metadata should be successfully
validated. Otherwise the ingest may be rejected. Select the schema and
the schemaVersion to validate given document.

    metadata-v3.xml:
    <?xml version='1.0' encoding='utf-8'?>
    <example:metadata xmlns:example="http://www.example.org/schema/xsd/" >
      <example:title>My third XML document</example:title>
      <example:date>2018-07-02</example:date>
      <example:note>since version 3 notes are allowed</example:note>
    </example:metadata>

On a first step validation with the old schema will be done:

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_xsd/validate?version=1' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'document=@metadata-v3.xml;type=application/xml' \
        -F 'version=1'

Same for the HTTP request. The schemaVersion number is set by a query
parameter.

    POST /api/v1/schemas/my_first_xsd/validate?version=1 HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=version

    1
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata-v3.xml
    Content-Type: application/xml

    <?xml version='1.0' encoding='utf-8'?>
    <example:metadata xmlns:example="http://www.example.org/schema/xsd/" >
      <example:title>My third XML document</example:title>
      <example:date>2018-07-02</example:date>
      <example:note>since version 3 notes are allowed</example:note>
    </example:metadata>
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As a result, you receive 422 as HTTP status and an error message holding
some information about the error. (Unfortunately not documented here)

    HTTP/1.1 422 Unprocessable Entity

The document holds a mandatory and an optional field introduced in the
second and third version of schema. Let’s try to validate with third
version of schema. Only version number will be different. (if no query
parameter is available the current version will be selected)

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_xsd/validate' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'document=@metadata-v3.xml;type=application/xml'

Same for the HTTP request.

    POST /api/v1/schemas/my_first_xsd/validate HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata-v3.xml
    Content-Type: application/xml

    <?xml version='1.0' encoding='utf-8'?>
    <example:metadata xmlns:example="http://www.example.org/schema/xsd/" >
      <example:title>My third XML document</example:title>
      <example:date>2018-07-02</example:date>
      <example:note>since version 3 notes are allowed</example:note>
    </example:metadata>
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

Everything should be fine now. As a result, you receive 204 as HTTP
status and no further content.

    HTTP/1.1 204 No Content

### Update Metadata Schema Record

In case of authorization it may be neccessary to update metadata record
to be accessible by others. To do so an update has to be made. In this
example we introduce a user called 'admin' and give him all rights.

    schema-record-v4.json
    {
      "schemaId":"my_first_xsd",
      "mimeType":"application/xml",
      "type":"XML",
      "acl":[
        {
          "id":33,
          "sid":"SELF",
          "permission":"ADMINISTRATE"
        },
        {
          "id":null,
          "sid":"admin",
          "admin":"ADMINISTRATE"
        }
      ]
    }

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_xsd' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "-1377314606"' \
        -F 'record=@schema-record-v4.json;type=application/json'

Same for the HTTP request.

    PUT /api/v1/schemas/my_first_xsd HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "-1377314606"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=schema-record-v4.json
    Content-Type: application/json

    {"schemaId":"my_first_xsd","pid":null,"schemaVersion":3,"label":null,"definition":null,"comment":null,"mimeType":"application/xml","type":"XML","createdAt":"2021-06-22T14:37:32Z","lastUpdate":"2021-06-22T14:37:33.594Z","acl":[{"id":1,"sid":"SELF","permission":"ADMINISTRATE"},{"id":null,"sid":"admin","permission":"ADMINISTRATE"}],"schemaDocumentUri":"http://localhost:8080/api/v1/schemas/my_first_xsd?version=3","schemaHash":null,"locked":false}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As a result, you receive 200 as HTTP status, the updated metadata schema
record and the updated ETag and location in the HTTP response header.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/schemas/my_first_xsd?version=4
    ETag: "-1943981230"
    Content-Type: application/json
    Content-Length: 465

    {
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 4,
      "mimeType" : "application/xml",
      "type" : "XML",
      "createdAt" : "2021-06-22T14:37:32Z",
      "lastUpdate" : "2021-06-22T14:37:34.342Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      }, {
        "id" : 3,
        "sid" : "admin",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_xsd?version=4",
      "locked" : false
    }

After the update the following fields has changed:

-   schemaVersion number increased by one.

-   lastUpdate to the date of the last update (set by server)

-   acl additional ACL entry (set during update)

Metadata Management
-------------------

After registration of a schema metadata may be added to MetaStore. In
this section, the handling of metadata resources is explained. It all
starts with creating your first metadata resource. The model of a
metadata record looks like this:

    {
      "id" : "...",
      "relatedResource" : "...",
      "createdAt" : "...",
      "lastUpdate" : "...",
      "recordVersion" : 1,
      "schemaId" : "...",
      "schemaVersion" : 1,
      "acl" : [ {
        "id" : 1,
        "sid" : "...",
        "permission" : "..."
      } ],
      "metadataDocumentUri" : "...",
      "documentHash" : "..."
    }

At least the following elements are expected to be provided by the user:

-   schemaId: A unique label of the schema.

-   relatedResource: The link to the resource.

In addition, ACL may be useful to make metadata editable by others.
(This will be of interest while updating an existing metadata)

### Register/Ingest a Metadata Record with Metadata Document

The following example shows the creation of the first metadata record
and its metadata only providing mandatory fields mentioned above:

    metadata-record.json:
    {
        "relatedResource": "anyResourceId",
        "schemaId": "my_first_xsd",
        "schemaVersion": "1"
    }

    metadata.xml:
    <?xml version='1.0' encoding='utf-8'?>
      <example:metadata xmlns:example="http://www.example.org/schema/xsd/" >
      <example:title>My first XML document</example:title>
    </example:metadata>

The schemaId used while registering metadata schema has to be used to
link the metadata with the approbriate metadata schema.

    $ curl 'http://localhost:8080/api/v1/metadata' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'record=@metadata-record.json;type=application/json' \
        -F 'document=@metadata.xml;type=application/xml'

You can see, that most of the sent metadata schema record is empty. Only
schemaId and relatedResource are provided by the user. HTTP-wise the
call looks as follows:

    POST /api/v1/metadata HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=metadata-record.json
    Content-Type: application/json

    {"id":null,"pid":null,"relatedResource":"anyResourceId","createdAt":null,"lastUpdate":null,"schemaId":"my_first_xsd","schemaVersion":1,"recordVersion":null,"acl":[],"metadataDocumentUri":null,"documentHash":null}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata.xml
    Content-Type: application/xml

    <?xml version='1.0' encoding='utf-8'?>
    <example:metadata xmlns:example="http://www.example.org/schema/xsd/" >
      <example:title>My first XML document</example:title>
    </example:metadata>
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As Content-Type only 'application/json' is supported and should be
provided. The other headers are typically set by the HTTP client. After
validating the provided document, adding missing information where
possible and persisting the created resource, the result is sent back to
the user and will look that way:

    HTTP/1.1 201 Created
    Location: http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1
    ETag: "-1320006500"
    Content-Type: application/json
    Content-Length: 523

    {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:34.726Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 1,
      "recordVersion" : 1,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1",
      "documentHash" : "sha1:ac92891f6377919446143e0a8639f12715397228"
    }

What you see is, that the metadata record looks different from the
original document. All remaining elements received a value by the
server. In the header you’ll find a location URL to access the ingested
metadata and an ETag with the current ETag of the resource. This value
is returned by POST, GET and PUT calls and must be provided for all
calls modifying the resource, e.g. POST, PUT and DELETE, in order to
avoid conflicts.

### Accessing Metadata Document

For accessing the metadata the location URL provided before may be used.
The URL is compiled by the id of the metadata and its version.

    $ curl 'http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1' -i -X GET \
        -H 'Accept: application/xml'

HTTP-wise the call looks as follows:

    GET /api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1 HTTP/1.1
    Accept: application/xml
    Host: localhost:8080

The linked metadata will be returned. The result is sent back to the
user and will look that way:

    HTTP/1.1 200 OK
    Content-Length: 198
    Accept-Ranges: bytes
    Content-Type: application/xml

    <?xml version="1.0" encoding="UTF-8"?>
    <example:metadata xmlns:example="http://www.example.org/schema/xsd/">

        <example:title>My first XML document</example:title>

    </example:metadata>

What you see is, that the metadata is untouched.

### Accessing Metadata Record

For accessing the metadata record the same URL as before has to be used.
The only difference is the content type. It has to be set to
"application/vnd.datamanager.metadata-record+json". Then the command
line looks like this:

    $ curl 'http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1' -i -X GET \
        -H 'Accept: application/vnd.datamanager.metadata-record+json'

HTTP-wise the call looks as follows:

    GET /api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1 HTTP/1.1
    Accept: application/vnd.datamanager.metadata-record+json
    Host: localhost:8080

The linked metadata will be returned. The result is sent back to the
user and will look that way:

    HTTP/1.1 200 OK
    ETag: "-1320006500"
    Content-Type: application/vnd.datamanager.metadata-record+json
    Content-Length: 523

    {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:34.726Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 1,
      "recordVersion" : 1,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1",
      "documentHash" : "sha1:ac92891f6377919446143e0a8639f12715397228"
    }

You also get the metadata record seen before.

### Updating a Metadata Record (edit ACL entries) & Metadata Document

The following example shows the update of the metadata record and
metadata document to a newer version of the schema. As mentioned before
the ETag is needed:

    metadata-record-v2.json:
    {
        "relatedResource": "anyResourceId",
        "schemaId": "my_first_xsd",
        "schemaVersion": "2",
        "acl": [ {
          "id":null,
          "sid":"guest",
          "permission":"READ"
        } ]
    }

    metadata-v2.xml:
    <?xml version='1.0' encoding='utf-8'?>
    <example:metadata xmlns:example="http://www.example.org/schema/xsd/" >
      <example:title>My second XML document</example:title>
      <example:date>2018-07-02</example:date>
    </example:metadata>

    $ curl 'http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "-1320006500"' \
        -F 'record=@metadata-record-v2.json;type=application/json' \
        -F 'document=@metadata-v2.xml;type=application/xml'

You can see, that only the ACL entry for "guest" was added. All other
properties are still the same. HTTP-wise the call looks as follows:

    PUT /api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "-1320006500"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=metadata-record-v2.json
    Content-Type: application/json

    {"id":"10a030fb-13e5-4a9d-bfab-d4efa48d252f","pid":null,"relatedResource":"anyResourceId","createdAt":"2021-06-22T14:37:34Z","lastUpdate":"2021-06-22T14:37:34.726Z","schemaId":"my_first_xsd","schemaVersion":2,"recordVersion":1,"acl":[{"id":4,"sid":"SELF","permission":"ADMINISTRATE"}],"metadataDocumentUri":"http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1","documentHash":"sha1:ac92891f6377919446143e0a8639f12715397228"}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata-v2.xml
    Content-Type: application/xml

    <?xml version='1.0' encoding='utf-8'?>
    <example:metadata xmlns:example="http://www.example.org/schema/xsd/" >
      <example:title>My second XML document</example:title>
      <example:date>2018-07-02</example:date>
    </example:metadata>
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

You will get the new metadata record with the additional ACL entry.
Version number was incremented by one, 'lastUpdate' and *recordVersion*
are also modified by the server.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=2
    ETag: "-1824244606"
    Content-Type: application/json
    Content-Length: 523

    {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:35.051Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 2,
      "recordVersion" : 2,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=2",
      "documentHash" : "sha1:e13a87884df391a611fb6257ea53883811d9451a"
    }

What you see is, that the metadata record looks different from the
original document.

### Updating Metadata Record & Document

Repeat the last step and update to the current version. As mentioned
before the ETag is needed. As the ETag has changed in the meanwhile you
first have to get the new ETag.

    $ curl 'http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=2' -i -X GET \
        -H 'Accept: application/vnd.datamanager.metadata-record+json'

HTTP-wise the call looks as follows:

    GET /api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=2 HTTP/1.1
    Accept: application/vnd.datamanager.metadata-record+json
    Host: localhost:8080

You will get the new metadata record with the new ETag.

    HTTP/1.1 200 OK
    ETag: "-1824244606"
    Content-Type: application/vnd.datamanager.metadata-record+json
    Content-Length: 523

    {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:35.051Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 2,
      "recordVersion" : 2,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=2",
      "documentHash" : "sha1:e13a87884df391a611fb6257ea53883811d9451a"
    }

Now you can update metadata due to new version of schema using the new
Etag.

    metadata-record-v3.json:
    {
        "relatedResource": "anyResourceId",
        "schemaId": "my_first_xsd",
        "schemaVersion": "3"
    }

    metadata-v3.xml:
    <?xml version='1.0' encoding='utf-8'?>
      <example:metadata xmlns:example="http://www.example.org/schema/xsd/" >
      <example:title>My third XML document</example:title>
      <example:date>2018-07-02</example:date>
      <example:note>since version 3 notes are allowed</example:note>
    </example:metadata>

    $ curl 'http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "-1824244606"' \
        -F 'record=@metadata-record-v3.json;type=application/json' \
        -F 'document=@metadata-v3.xml;type=application/xml'

HTTP-wise the call looks as follows:

    PUT /api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "-1824244606"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=metadata-record-v3.json
    Content-Type: application/json

    {"id":"10a030fb-13e5-4a9d-bfab-d4efa48d252f","pid":null,"relatedResource":"anyResourceId","createdAt":"2021-06-22T14:37:34Z","lastUpdate":"2021-06-22T14:37:34.726Z","schemaId":"my_first_xsd","schemaVersion":3,"recordVersion":1,"acl":[{"id":4,"sid":"SELF","permission":"ADMINISTRATE"}],"metadataDocumentUri":"http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1","documentHash":"sha1:ac92891f6377919446143e0a8639f12715397228"}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata-v3.xml
    Content-Type: application/xml

    <?xml version='1.0' encoding='utf-8'?>
    <example:metadata xmlns:example="http://www.example.org/schema/xsd/" >
      <example:title>My third XML document</example:title>
      <example:date>2018-07-02</example:date>
      <example:note>since version 3 notes are allowed</example:note>
    </example:metadata>
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

You will get the new metadata record.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=3
    ETag: "-155583553"
    Content-Type: application/json
    Content-Length: 522

    {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:35.25Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 3,
      "recordVersion" : 3,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=3",
      "documentHash" : "sha1:55547a0ad07445cfbc11a76484da3b21d23ceb82"
    }

Now you can access the updated metadata via the URI in the HTTP response
header.

    $ curl 'http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=3' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=3 HTTP/1.1
    Host: localhost:8080

You will get the updated metadata.

    HTTP/1.1 200 OK
    Content-Length: 323
    Accept-Ranges: bytes
    Content-Type: application/json

    <?xml version="1.0" encoding="UTF-8"?>
    <example:metadata xmlns:example="http://www.example.org/schema/xsd/">

        <example:title>My third XML document</example:title>

        <example:date>2018-07-02</example:date>

        <example:note>since version 3 notes are allowed</example:note>

    </example:metadata>

### Find a Metadata Record

Search will find all current metadata records. There are some filters
available which may be combined. All filters for the metadata records
are set via query parameters. The following filters are allowed:

-   id

-   resourceId

-   from

-   until

> **Note**
>
> The header contains the field 'Content-Range" which displays delivered
> indices and the maximum number of available schema records. If there
> are more than 20 metadata records registered you have to provide page
> and/or size as additional query parameters.

-   page: Number of the page you want to get **(starting with page 0)**

-   size: Number of entries per page.

### Getting a List of all Metadata Records for a Specific Metadata Document

If you want to obtain all versions of a specific resource you may add
'id' as a filter parameter. This may look like this:

    $ curl 'http://localhost:8080/api/v1/metadata?id=10a030fb-13e5-4a9d-bfab-d4efa48d252f' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata?id=10a030fb-13e5-4a9d-bfab-d4efa48d252f HTTP/1.1
    Host: localhost:8080

As a result, you receive a list of metadata records in descending order.
(current version first)

    HTTP/1.1 200 OK
    Content-Range: 0-19/3
    Content-Type: application/json
    Content-Length: 1576

    [ {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:35.25Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 3,
      "recordVersion" : 3,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=3",
      "documentHash" : "sha1:55547a0ad07445cfbc11a76484da3b21d23ceb82"
    }, {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:35.051Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 2,
      "recordVersion" : 2,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=2",
      "documentHash" : "sha1:55547a0ad07445cfbc11a76484da3b21d23ceb82"
    }, {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:34.726Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 1,
      "recordVersion" : 1,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=1",
      "documentHash" : "sha1:55547a0ad07445cfbc11a76484da3b21d23ceb82"
    } ]

#### Find by resourceId

If you want to find all records belonging to an external resource.
Metastore may hold multiple metadata documents per resource.
(Nevertheless only one per registered schema)

Command line:

    $ curl 'http://localhost:8080/api/v1/metadata?resoureId=anyResourceId' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata?resoureId=anyResourceId HTTP/1.1
    Host: localhost:8080

You will get the current version of the metadata record(s).

    HTTP/1.1 200 OK
    Content-Range: 0-19/1
    Content-Type: application/json
    Content-Length: 526

    [ {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:35.25Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 3,
      "recordVersion" : 3,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=3",
      "documentHash" : "sha1:55547a0ad07445cfbc11a76484da3b21d23ceb82"
    } ]

#### Find after a specific date

If you want to find all metadata records updated after a specific date.

Command line:

    $ curl 'http://localhost:8080/api/v1/metadata?from=2021-06-22T12%3A37%3A35.493178Z' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata?from=2021-06-22T12%3A37%3A35.493178Z HTTP/1.1
    Host: localhost:8080

You will get the current version metadata records updated ln the last 2
hours.

    HTTP/1.1 200 OK
    Content-Range: 0-19/1
    Content-Type: application/json
    Content-Length: 526

    [ {
      "id" : "10a030fb-13e5-4a9d-bfab-d4efa48d252f",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:34Z",
      "lastUpdate" : "2021-06-22T14:37:35.25Z",
      "schemaId" : "my_first_xsd",
      "schemaVersion" : 3,
      "recordVersion" : 3,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/10a030fb-13e5-4a9d-bfab-d4efa48d252f?version=3",
      "documentHash" : "sha1:55547a0ad07445cfbc11a76484da3b21d23ceb82"
    } ]

#### Find in a specific date range

If you want to find all metadata records updated in a specific date
range.

Command line:

    $ curl 'http://localhost:8080/api/v1/metadata?from=2021-06-22T12%3A37%3A35.493178Z&until=2021-06-22T13%3A37%3A35.493162Z' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata?from=2021-06-22T12%3A37%3A35.493178Z&until=2021-06-22T13%3A37%3A35.493162Z HTTP/1.1
    Host: localhost:8080

You will get an empty array as no metadata record exists in the given
range:

    HTTP/1.1 200 OK
    Content-Range: 0-19/0
    Content-Type: application/json
    Content-Length: 3

    [ ]

JSON (Schema)
=============

Schema Registration and Management
----------------------------------

In this section, the handling of json schema resources is explained. It
all starts with creating your first json schema resource. The model of a
metadata schema record looks like this:

    {
      "schemaId" : "...",
      "schemaVersion" : 1,
      "mimeType" : "...",
      "type" : "...",
      "createdAt" : "...",
      "lastUpdate" : "...",
      "acl" : [ {
        "id" : 1,
        "sid" : "...",
        "permission" : "..."
      } ],
      "schemaDocumentUri" : "...",
      "schemaHash" : "...",
      "locked" : false
    }

At least the following elements are expected to be provided by the user:

-   schemaId: A unique label for the schema.

-   mimeType: The resource type must be assigned by the user. For JSON
    schemas this should be *application/json*

-   type: XML or JSON. For JSON schemas this should be *JSON*

In addition, ACL may be useful to make schema editable by others. (This
will be of interest while updating an existing schema)

Registering a Metadata Schema Document
--------------------------------------

The following example shows the creation of the first json schema only
providing mandatory fields mentioned above:

    schema-record4json.json:
    {
      "schemaId" : "my_first_json",
      "mimeType" : "application/json",
      "type" : "JSON"
    }

    schema.json:
    {
      "$schema": "http://json-schema.org/draft/2019-09/schema#",
      "$id": "http://www.example.org/schema/json",
      "type": "object",
      "title": "Json schema for tests",
      "default": {},
      "required": [
          "title"
      ],
      "properties": {
        "title": {
          "$id": "#/properties/string",
          "type": "string",
          "title": "Title",
          "description": "Title of object."
        }
      },
      "additionalProperties": false
    }

    $ curl 'http://localhost:8080/api/v1/schemas' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'schema=@schema.json;type=application/json' \
        -F 'record=@schema-record4json.json;type=application/json'

You can see, that most of the sent metadata schema record is empty. Only
schemaId, mimeType and type are provided by the user. HTTP-wise the call
looks as follows:

    POST /api/v1/schemas HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=schema; filename=schema.json
    Content-Type: application/json

    {
        "$schema": "http://json-schema.org/draft/2019-09/schema#",
        "$id": "http://www.example.org/schema/json",
        "type": "object",
        "title": "Json schema for tests",
        "default": {},
        "required": [
            "title"
        ],
        "properties": {
            "title": {
                "$id": "#/properties/string",
                "type": "string",
                "title": "Title",
                "description": "Title of object."
            }
        },
        "additionalProperties": false
    }
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=schema-record4json.json
    Content-Type: application/json

    {"schemaId":"my_first_json","pid":null,"schemaVersion":null,"label":null,"definition":null,"comment":null,"mimeType":"application/json","type":"JSON","createdAt":null,"lastUpdate":null,"acl":[],"schemaDocumentUri":null,"schemaHash":null,"locked":false}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As Content-Type only 'application/json' is supported and should be
provided. The other headers are typically set by the HTTP client. After
validating the provided document, adding missing information where
possible and persisting the created resource, the result is sent back to
the user and will look that way:

    HTTP/1.1 201 Created
    Location: http://localhost:8080/api/v1/schemas/my_first_json?version=1
    ETag: "-698196229"
    Content-Type: application/json
    Content-Length: 393

    {
      "schemaId" : "my_first_json",
      "schemaVersion" : 1,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:42Z",
      "lastUpdate" : "2021-06-22T14:37:42.099Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_json?version=1",
      "locked" : false
    }

What you see is, that the metadata schema record looks different from
the original document. All remaining elements received a value by the
server. Furthermore, you’ll find an ETag header with the current ETag of
the resource. This value is returned by POST, GET and PUT calls and must
be provided for all calls modifying the resource, e.g. POST, PUT and
DELETE, in order to avoid conflicts.

### Getting a Metadata Schema Record

For obtaining one metadata schema record you have to provide the value
of the field 'schemaId'.

> **Note**
>
> As 'Accept' field you have to provide
> 'application/vnd.datamanager.schema-record+json' otherwise you will
> get the metadata schema instead.

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_json' -i -X GET \
        -H 'Accept: application/vnd.datamanager.schema-record+json'

In the actual HTTP request just access the path of the resource using
the base path and the 'schemaid'. Be aware that you also have to provide
the 'Accept' field.

    GET /api/v1/schemas/my_first_json HTTP/1.1
    Accept: application/vnd.datamanager.schema-record+json
    Host: localhost:8080

As a result, you receive the metadata schema record send before and
again the corresponding ETag in the HTTP response header.

    HTTP/1.1 200 OK
    ETag: "-698196229"
    Content-Type: application/vnd.datamanager.schema-record+json
    Content-Length: 393

    {
      "schemaId" : "my_first_json",
      "schemaVersion" : 1,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:42Z",
      "lastUpdate" : "2021-06-22T14:37:42.099Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_json?version=1",
      "locked" : false
    }

### Getting a Metadata Schema Document

For obtaining accessible metadata schemas you also have to provide the
'schemaId'. For accessing schema document you don’t have to provide the
'Accept' header.

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_json' -i -X GET

In the actual HTTP request there is nothing special. You just access the
path of the resource using the base path and the 'schemaId'.

    GET /api/v1/schemas/my_first_json HTTP/1.1
    Host: localhost:8080

As a result, you receive the XSD schema send before.

    HTTP/1.1 200 OK
    Content-Type: application/json
    Content-Length: 425
    Accept-Ranges: bytes

    {
      "$schema" : "http://json-schema.org/draft/2019-09/schema#",
      "$id" : "http://www.example.org/schema/json",
      "type" : "object",
      "title" : "Json schema for tests",
      "default" : { },
      "required" : [ "title" ],
      "properties" : {
        "title" : {
          "$id" : "#/properties/string",
          "type" : "string",
          "title" : "Title",
          "description" : "Title of object."
        }
      },
      "additionalProperties" : false
    }

### Updating a Metadata Schema Document (add mandatory 'date' field)

> **Note**
>
> Updating a metadata schema document will not break old metadata
> documents. As every update results in a new version 'old' metadata
> schema documents are still available.

For updating an existing metadata schema (record) a valid ETag is
needed. The actual ETag is available via the HTTP GET call of the
metadata schema record. (see above) Just send an HTTP POST with the
updated metadata schema document and/or metadata schema record.

    schema-v2.json:
    {
        "$schema": "http://json-schema.org/draft/2019-09/schema#",
        "$id": "http://www.example.org/schema/json",
        "type": "object",
        "title": "Json schema for tests",
        "default": {},
        "required": [
            "title",
            "date"
        ],
        "properties": {
            "title": {
                "$id": "#/properties/string",
                "type": "string",
                "title": "Title",
                "description": "Title of object."
            },
            "date": {
                "$id": "#/properties/string",
                "type": "string",
                "format": "date",
                "title": "Date",
                "description": "Date of object"
            }
        },
        "additionalProperties": false
    }

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_json' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "-698196229"' \
        -F 'schema=@schema-v2.json;type=application/json'

HTTP-wise the call looks as follows:

    PUT /api/v1/schemas/my_first_json HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "-698196229"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=schema; filename=schema-v2.json
    Content-Type: application/json

    {
        "$schema": "http://json-schema.org/draft/2019-09/schema#",
        "$id": "http://www.example.org/schema/json",
        "type": "object",
        "title": "Json schema for tests",
        "default": {},
        "required": [
            "title",
            "date"
        ],
        "properties": {
            "title": {
                "$id": "#/properties/string",
                "type": "string",
                "title": "Title",
                "description": "Title of object."
            },
            "date": {
                "$id": "#/properties/string",
                "type": "string",
                "format": "date",
                "title": "Date",
                "description": "Date of object"
            }
        },
        "additionalProperties": false
    }
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As a result, you receive the updated schema record and in the HTTP
response header the new location URL and the ETag.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/schemas/my_first_json?version=2
    ETag: "948622444"
    Content-Type: application/json
    Content-Length: 393

    {
      "schemaId" : "my_first_json",
      "schemaVersion" : 2,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:42Z",
      "lastUpdate" : "2021-06-22T14:37:42.268Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_json?version=2",
      "locked" : false
    }

### Updating a Metadata Schema Document (add optional 'note' field)

For updating existing metadata schema document we have to provide the
new ETag. Just send an HTTP POST with the updated metadata schema
document and/or metadata schema record.

    schema-v3.json:
    {
        "$schema": "http://json-schema.org/draft/2019-09/schema#",
        "$id": "http://www.example.org/schema/json",
        "type": "object",
        "title": "Json schema for tests",
        "default": {},
        "required": [
            "title",
            "date"
        ],
        "properties": {
            "title": {
                "$id": "#/properties/string",
                "type": "string",
                "title": "Title",
                "description": "Title of object."
            },
            "date": {
                "$id": "#/properties/string",
                "type": "string",
                "format": "date",
                "title": "Date",
                "description": "Date of object"
            },
            "note": {
                "$id": "#/properties/string",
                "type": "string",
                "title": "Note",
                "description": "Additonal information about object"
            }
        },
        "additionalProperties": false
    }

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_json' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "948622444"' \
        -F 'schema=@schema-v3.json;type=application/json'

HTTP-wise the call looks as follows:

    PUT /api/v1/schemas/my_first_json HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "948622444"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=schema; filename=schema-v3.json
    Content-Type: application/json

    {
        "$schema": "http://json-schema.org/draft/2019-09/schema#",
        "$id": "http://www.example.org/schema/json",
        "type": "object",
        "title": "Json schema for tests",
        "default": {},
        "required": [
            "title",
            "date"
        ],
        "properties": {
            "title": {
                "$id": "#/properties/string",
                "type": "string",
                "title": "Title",
                "description": "Title of object."
            },
            "date": {
                "$id": "#/properties/string",
                "type": "string",
                "format": "date",
                "title": "Date",
                "description": "Date of object"
            },
            "note": {
                "$id": "#/properties/string",
                "type": "string",
                "title": "Note",
                "description": "Additonal information about object"
            }
        },
        "additionalProperties": false
    }
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As a result, you receive the updated schema record and in the HTTP
response header the new location URL and the ETag.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/schemas/my_first_json?version=3
    ETag: "-1123639459"
    Content-Type: application/json
    Content-Length: 393

    {
      "schemaId" : "my_first_json",
      "schemaVersion" : 3,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:42Z",
      "lastUpdate" : "2021-06-22T14:37:42.807Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_json?version=3",
      "locked" : false
    }

The updated schema record contains three modified fields:
'schemaVersion', 'lastUpdate' and 'schemaDocumentUri'.

Registering another Metadata Schema Document
--------------------------------------------

The following example shows the creation of another json schema only
providing mandatory fields mentioned above:

    another-schema-record4json.json:
    {
      "schemaId" : "another_json",
      "mimeType" : "application/json",
      "type" : "JSON"
    }

    another-schema.json:
    {
        "$schema": "http://json-schema.org/draft/2019-09/schema#",
        "$id": "http://www.example.org/schema/json/example",
        "type": "object",
        "title": "Another Json schema for tests",
        "default": {},
        "required": [
            "description"
        ],
        "properties": {
            "description": {
                "$id": "#/properties/string",
                "type": "string",
                "title": "Description",
                "description": "Any description."
            }
        },
        "additionalProperties": false
    }

    $ curl 'http://localhost:8080/api/v1/schemas' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'schema=@another-schema.json;type=application/xml' \
        -F 'record=@another-schema-record.json;type=application/json'

HTTP-wise the call looks as follows:

    POST /api/v1/schemas HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=schema; filename=another-schema.json
    Content-Type: application/xml

    {
        "$schema": "http://json-schema.org/draft/2019-09/schema#",
        "$id": "http://www.example.org/schema/json/example",
        "type": "object",
        "title": "Another Json schema for tests",
        "default": {},
        "required": [
            "description"
        ],
        "properties": {
            "description": {
                "$id": "#/properties/string",
                "type": "string",
                "title": "Description",
                "description": "Any description."
            }
        },
        "additionalProperties": false
    }
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=another-schema-record.json
    Content-Type: application/json

    {"schemaId":"another_json","pid":null,"schemaVersion":null,"label":null,"definition":null,"comment":null,"mimeType":"application/json","type":"JSON","createdAt":null,"lastUpdate":null,"acl":[],"schemaDocumentUri":null,"schemaHash":null,"locked":false}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As Content-Type only 'application/json' is supported and should be
provided. The other headers are typically set by the HTTP client. After
validating the provided document, adding missing information where
possible and persisting the created resource, the result is sent back to
the user and will look that way:

    HTTP/1.1 201 Created
    Location: http://localhost:8080/api/v1/schemas/another_json?version=1
    ETag: "1029860118"
    Content-Type: application/json
    Content-Length: 391

    {
      "schemaId" : "another_json",
      "schemaVersion" : 1,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:43.633Z",
      "acl" : [ {
        "id" : 2,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/another_json?version=1",
      "locked" : false
    }

Now there are two schemaIds registered in the metadata schema registry.

### Getting a List of Metadata Schema Records

Obtaining all accessible metadata schema records.

    $ curl 'http://localhost:8080/api/v1/schemas' -i -X GET

Same for HTTP request:

    GET /api/v1/schemas HTTP/1.1
    Host: localhost:8080

As a result, you receive a list of metadata schema records.

    HTTP/1.1 200 OK
    Content-Range: 0-19/2
    Content-Type: application/json
    Content-Length: 790

    [ {
      "schemaId" : "another_json",
      "schemaVersion" : 1,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:43.633Z",
      "acl" : [ {
        "id" : 2,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/another_json?version=1",
      "locked" : false
    }, {
      "schemaId" : "my_first_json",
      "schemaVersion" : 3,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:42Z",
      "lastUpdate" : "2021-06-22T14:37:42.807Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_json?version=3",
      "locked" : false
    } ]

> **Note**
>
> Only the current version of each schemaId is listed.

> **Note**
>
> The header contains the field 'Content-Range" which displays delivered
> indices and the maximum number of available schema records. If there
> are more than 20 schemas registered you have to provide page and/or
> size as additional query parameters.

-   page: Number of the page you want to get **(starting with page 0)**

-   size: Number of entries per page.

The modified HTTP request with pagination looks like follows:

    GET /api/v1/schemas?page=0&size=20 HTTP/1.1
    Host: localhost:8080

### Getting a List of all Schema Records for a Specific SchemaId

If you want to obtain all versions of a specific schema you may add the
schemaId as a filter parameter. This may look like this:

    $ curl 'http://localhost:8080/api/v1/schemas?schemaId=my_first_json' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/schemas?schemaId=my_first_json HTTP/1.1
    Host: localhost:8080

As a result, you receive a list of metadata schema records in descending
order. (current version first)

    HTTP/1.1 200 OK
    Content-Range: 0-19/3
    Content-Type: application/json
    Content-Length: 1187

    [ {
      "schemaId" : "my_first_json",
      "schemaVersion" : 3,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:42Z",
      "lastUpdate" : "2021-06-22T14:37:42.807Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_json?version=3",
      "locked" : false
    }, {
      "schemaId" : "my_first_json",
      "schemaVersion" : 2,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:42Z",
      "lastUpdate" : "2021-06-22T14:37:42.268Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_json?version=2",
      "locked" : false
    }, {
      "schemaId" : "my_first_json",
      "schemaVersion" : 1,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:42Z",
      "lastUpdate" : "2021-06-22T14:37:42.099Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_json?version=1",
      "locked" : false
    } ]

### Getting current Version of Metadata Schema Document

To get the current version of the metadata schema document just send an
HTTP GET with the linked 'schemaId':

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_json' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/schemas/my_first_json HTTP/1.1
    Host: localhost:8080

As a result, you receive the XSD schema document sent before.

    HTTP/1.1 200 OK
    Content-Type: application/json
    Content-Length: 772
    Accept-Ranges: bytes

    {
      "$schema" : "http://json-schema.org/draft/2019-09/schema#",
      "$id" : "http://www.example.org/schema/json",
      "type" : "object",
      "title" : "Json schema for tests",
      "default" : { },
      "required" : [ "title", "date" ],
      "properties" : {
        "title" : {
          "$id" : "#/properties/string",
          "type" : "string",
          "title" : "Title",
          "description" : "Title of object."
        },
        "date" : {
          "$id" : "#/properties/string",
          "type" : "string",
          "format" : "date",
          "title" : "Date",
          "description" : "Date of object"
        },
        "note" : {
          "$id" : "#/properties/string",
          "type" : "string",
          "title" : "Note",
          "description" : "Additonal information about object"
        }
      },
      "additionalProperties" : false
    }

### Getting a specific Version of Metadata Schema Document

To get a specific version of the metadata schema document just send an
HTTP GET with the linked 'schemaId' and the version number you are
looking for as query parameter:

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_json?version=1' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/schemas/my_first_json?version=1 HTTP/1.1
    Host: localhost:8080

As a result, you receive the initial XSD schema document (version 1).

    HTTP/1.1 200 OK
    Content-Type: application/json
    Content-Length: 425
    Accept-Ranges: bytes

    {
      "$schema" : "http://json-schema.org/draft/2019-09/schema#",
      "$id" : "http://www.example.org/schema/json",
      "type" : "object",
      "title" : "Json schema for tests",
      "default" : { },
      "required" : [ "title" ],
      "properties" : {
        "title" : {
          "$id" : "#/properties/string",
          "type" : "string",
          "title" : "Title",
          "description" : "Title of object."
        }
      },
      "additionalProperties" : false
    }

### Validating Metadata Document

Before an ingest of metadata is made the metadata should be successfully
validated. Otherwise the ingest may be rejected. Select the schema and
the schemaVersion to validate given document.

    metadata-v3.json:
    {
    "title": "My third JSON document",
    "date": "2018-07-02",
    "note": "since version 3 notes are allowed"
    }

On a first step validation with the old schema will be done:

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_json/validate?version=1' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'document=@metadata-v3.json;type=application/json' \
        -F 'version=1'

Same for the HTTP request. The schemaVersion number is set by a query
parameter.

    POST /api/v1/schemas/my_first_json/validate?version=1 HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=version

    1
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata-v3.json
    Content-Type: application/json

    {
    "title": "My third JSON document",
    "date": "2018-07-02",
    "note": "since version 3 notes are allowed"
    }
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As a result, you receive 422 as HTTP status and an error message holding
some information about the error. (Unfortunately not documented here)

    HTTP/1.1 422 Unprocessable Entity

The document holds a mandatory and an optional field introduced in the
second and third version of schema. Let’s try to validate with third
version of schema. Only version number will be different. (if no query
parameter is available the current version will be selected)

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_json/validate' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'document=@metadata-v3.json;type=application/json'

Same for the HTTP request.

    POST /api/v1/schemas/my_first_json/validate HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata-v3.json
    Content-Type: application/json

    {
    "title": "My third JSON document",
    "date": "2018-07-02",
    "note": "since version 3 notes are allowed"
    }
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

Everything should be fine now. As a result, you receive 204 as HTTP
status and no further content.

    HTTP/1.1 204 No Content

### Update Metadata Schema Record

In case of authorization it may be neccessary to update metadata record
to be accessible by others. To do so an update has to be made. In this
example we introduce a user called 'admin' and give him all rights.

    schema-record4json-v4.json
    {
      "schemaId" : "my_first_json",
      "mimeType" : "application/json",
      "type" : "JSON",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      }, {
        "id" : 3,
        "sid" : "admin",
        "permission" : "ADMINISTRATE"
      } ]
    }

    $ curl 'http://localhost:8080/api/v1/schemas/my_first_json' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "-1123639459"' \
        -F 'record=@schema-record4json-v4.json;type=application/json'

Same for the HTTP request.

    PUT /api/v1/schemas/my_first_json HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "-1123639459"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=schema-record4json-v4.json
    Content-Type: application/json

    {"schemaId":"my_first_json","pid":null,"schemaVersion":3,"label":null,"definition":null,"comment":null,"mimeType":"application/json","type":"JSON","createdAt":"2021-06-22T14:37:42Z","lastUpdate":"2021-06-22T14:37:42.807Z","acl":[{"id":1,"sid":"SELF","permission":"ADMINISTRATE"},{"id":null,"sid":"admin","permission":"ADMINISTRATE"}],"schemaDocumentUri":"http://localhost:8080/api/v1/schemas/my_first_json?version=3","schemaHash":null,"locked":false}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As a result, you receive 200 as HTTP status, the updated metadata schema
record and the updated ETag and location in the HTTP response header.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/schemas/my_first_json?version=4
    ETag: "-2075746403"
    Content-Type: application/json
    Content-Length: 469

    {
      "schemaId" : "my_first_json",
      "schemaVersion" : 4,
      "mimeType" : "application/json",
      "type" : "JSON",
      "createdAt" : "2021-06-22T14:37:42Z",
      "lastUpdate" : "2021-06-22T14:37:43.904Z",
      "acl" : [ {
        "id" : 1,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      }, {
        "id" : 3,
        "sid" : "admin",
        "permission" : "ADMINISTRATE"
      } ],
      "schemaDocumentUri" : "http://localhost:8080/api/v1/schemas/my_first_json?version=4",
      "locked" : false
    }

After the update the following fields has changed:

-   schemaVersion number increased by one.

-   lastUpdate to the date of the last update (set by server)

-   acl additional ACL entry (set during update)

Metadata Management
-------------------

After registration of a schema metadata may be added to MetaStore. In
this section, the handling of metadata resources is explained. It all
starts with creating your first metadata resource. The model of a
metadata record looks like this:

    {
      "id" : "...",
      "relatedResource" : "...",
      "createdAt" : "...",
      "lastUpdate" : "...",
      "recordVersion" : 1,
      "schemaId" : "...",
      "schemaVersion" : 1,
      "acl" : [ {
        "id" : 1,
        "sid" : "...",
        "permission" : "..."
      } ],
      "metadataDocumentUri" : "...",
      "documentHash" : "..."
    }

At least the following elements are expected to be provided by the user:

-   schemaId: A unique label of the schema.

-   relatedResource: The link to the resource.

In addition, ACL may be useful to make metadata editable by others.
(This will be of interest while updating an existing metadata)

### Register/Ingest a Metadata Record with Metadata Document

The following example shows the creation of the first metadata record
and its metadata only providing mandatory fields mentioned above:

    metadata-record4json.json:
    {
        "relatedResource": "anyResourceId",
        "schemaId": "my_first_json",
        "schemaVersion": "1"
    }

    metadata.json:
    {
      "title": "My first JSON document"
    }

The schemaId used while registering metadata schema has to be used to
link the metadata with the approbriate metadata schema.

    $ curl 'http://localhost:8080/api/v1/metadata' -i -X POST \
        -H 'Content-Type: multipart/form-data' \
        -F 'record=@metadata-record4json.json;type=application/json' \
        -F 'document=@metadata.json;type=application/json'

You can see, that most of the sent metadata schema record is empty. Only
schemaId and relatedResource are provided by the user. HTTP-wise the
call looks as follows:

    POST /api/v1/metadata HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=metadata-record4json.json
    Content-Type: application/json

    {"id":null,"pid":null,"relatedResource":"anyResourceId","createdAt":null,"lastUpdate":null,"schemaId":"my_first_json","schemaVersion":1,"recordVersion":null,"acl":[],"metadataDocumentUri":null,"documentHash":null}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata.json
    Content-Type: application/json

    {
    "title": "My first JSON document"
    }
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

As Content-Type only 'application/json' is supported and should be
provided. The other headers are typically set by the HTTP client. After
validating the provided document, adding missing information where
possible and persisting the created resource, the result is sent back to
the user and will look that way:

    HTTP/1.1 201 Created
    Location: http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1
    ETag: "-516989353"
    Content-Type: application/json
    Content-Length: 524

    {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:43.979Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 1,
      "recordVersion" : 1,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1",
      "documentHash" : "sha1:97ac2fb17cd40aac07a55444dc161d615c70af8a"
    }

What you see is, that the metadata record looks different from the
original document. All remaining elements received a value by the
server. In the header you’ll find a location URL to access the ingested
metadata and an ETag with the current ETag of the resource. This value
is returned by POST, GET and PUT calls and must be provided for all
calls modifying the resource, e.g. POST, PUT and DELETE, in order to
avoid conflicts.

### Accessing Metadata Document

For accessing the metadata the location URL provided before may be used.
The URL is compiled by the id of the metadata and its version.

    $ curl 'http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1' -i -X GET \
        -H 'Accept: application/json'

HTTP-wise the call looks as follows:

    GET /api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1 HTTP/1.1
    Accept: application/json
    Host: localhost:8080

The linked metadata will be returned. The result is sent back to the
user and will look that way:

    HTTP/1.1 200 OK
    Content-Length: 40
    Accept-Ranges: bytes
    Content-Type: application/json

    {
      "title" : "My first JSON document"
    }

What you see is, that the metadata is untouched.

### Accessing Metadata Record

For accessing the metadata record the same URL as before has to be used.
The only difference is the content type. It has to be set to
"application/vnd.datamanager.metadata-record+json". Then the command
line looks like this:

    $ curl 'http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1' -i -X GET \
        -H 'Accept: application/vnd.datamanager.metadata-record+json'

HTTP-wise the call looks as follows:

    GET /api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1 HTTP/1.1
    Accept: application/vnd.datamanager.metadata-record+json
    Host: localhost:8080

The linked metadata will be returned. The result is sent back to the
user and will look that way:

    HTTP/1.1 200 OK
    ETag: "-516989353"
    Content-Type: application/vnd.datamanager.metadata-record+json
    Content-Length: 524

    {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:43.979Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 1,
      "recordVersion" : 1,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1",
      "documentHash" : "sha1:97ac2fb17cd40aac07a55444dc161d615c70af8a"
    }

You also get the metadata record seen before.

### Updating a Metadata Record (edit ACL entries)

The following example shows the update of the metadata record. As
mentioned before the ETag is needed:

    metadata-record4json-v2.json:
    {
        "relatedResource": "anyResourceId",
        "schemaId": "my_first_json",
        "schemaVersion": "2",
        "acl": [ {
          "id":null,
          "sid":"guest",
          "permission":"READ"
        } ]
    }

    metadata-v2.json:
    {
    "title": "My second JSON document",
    "date": "2018-07-02"
    }

    $ curl 'http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "-516989353"' \
        -F 'record=@metadata-record4json-v2.json;type=application/json' \
        -F 'document=@metadata-v2.json;type=application/xml' \
        -F 'version=1'

You can see, that only the ACL entry for "guest" was added. All other
properties are still the same. HTTP-wise the call looks as follows:

    PUT /api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1 HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "-516989353"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=version

    1
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=metadata-record4json-v2.json
    Content-Type: application/json

    {"id":"ccb78474-b00e-4792-a59d-772c40d037b5","pid":null,"relatedResource":"anyResourceId","createdAt":"2021-06-22T14:37:43Z","lastUpdate":"2021-06-22T14:37:43.979Z","schemaId":"my_first_json","schemaVersion":2,"recordVersion":1,"acl":[{"id":4,"sid":"SELF","permission":"ADMINISTRATE"}],"metadataDocumentUri":"http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1","documentHash":"sha1:97ac2fb17cd40aac07a55444dc161d615c70af8a"}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata-v2.json
    Content-Type: application/xml

    {
    "title": "My second JSON document",
    "date": "2018-07-02"
    }
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

You will get the new metadata record with the additional ACL entry.
Version number was incremented by one, 'lastUpdate' and *recordVersion*
are also modified by the server.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=2
    ETag: "219202749"
    Content-Type: application/json
    Content-Length: 524

    {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:44.206Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 2,
      "recordVersion" : 2,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=2",
      "documentHash" : "sha1:1844c8057b673ae260fcc6b6ba146529b2b52771"
    }

What you see is, that the metadata record looks different from the
original document.

### Updating Metadata Record & Document

Repeat the last step and update to the current version. As mentioned
before the ETag is needed. As the ETag has changed in the meanwhile you
first have to get the new ETag.

    $ curl 'http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=2' -i -X GET \
        -H 'Accept: application/vnd.datamanager.metadata-record+json'

HTTP-wise the call looks as follows:

    GET /api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=2 HTTP/1.1
    Accept: application/vnd.datamanager.metadata-record+json
    Host: localhost:8080

You will get the new metadata record with the new ETag.

    HTTP/1.1 200 OK
    ETag: "219202749"
    Content-Type: application/vnd.datamanager.metadata-record+json
    Content-Length: 524

    {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:44.206Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 2,
      "recordVersion" : 2,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=2",
      "documentHash" : "sha1:1844c8057b673ae260fcc6b6ba146529b2b52771"
    }

Now you can update metadata due to new version of schema using the new
Etag.

    metadata-record4json-v3.json:
    {
      "relatedResource": "anyResourceId",
      "schemaId": "my_first_json",
      "schemaVersion": "3"
    }

    metadata-v3.json:
    {
    "title": "My third JSON document",
    "date": "2018-07-02",
    "note": "since version 3 notes are allowed"
    }

    $ curl 'http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5' -i -X PUT \
        -H 'Content-Type: multipart/form-data' \
        -H 'If-Match: "219202749"' \
        -F 'record=@metadata-record4json-v3.json;type=application/json' \
        -F 'document=@metadata-v3.json;type=application/xml'

HTTP-wise the call looks as follows:

    PUT /api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5 HTTP/1.1
    Content-Type: multipart/form-data; boundary=6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    If-Match: "219202749"
    Host: localhost:8080

    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=record; filename=metadata-record4json-v3.json
    Content-Type: application/json

    {"id":"ccb78474-b00e-4792-a59d-772c40d037b5","pid":null,"relatedResource":"anyResourceId","createdAt":"2021-06-22T14:37:43Z","lastUpdate":"2021-06-22T14:37:43.979Z","schemaId":"my_first_json","schemaVersion":3,"recordVersion":1,"acl":[{"id":4,"sid":"SELF","permission":"ADMINISTRATE"}],"metadataDocumentUri":"http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1","documentHash":"sha1:97ac2fb17cd40aac07a55444dc161d615c70af8a"}
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm
    Content-Disposition: form-data; name=document; filename=metadata-v3.json
    Content-Type: application/xml

    {
    "title": "My third JSON document",
    "date": "2018-07-02",
    "note": "since version 3 notes are allowed"
    }
    --6o2knFse3p53ty9dmcQvWAIx1zInP11uCfbm--

You will get the new metadata record.

    HTTP/1.1 200 OK
    Location: http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=3
    ETag: "868948090"
    Content-Type: application/json
    Content-Length: 524

    {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:44.347Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 3,
      "recordVersion" : 3,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=3",
      "documentHash" : "sha1:737762db675032231ac3cb872fccd32a83ac24d1"
    }

Now you can access the updated metadata via the URI in the HTTP response
header.

    $ curl 'http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=3' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=3 HTTP/1.1
    Host: localhost:8080

You will get the updated metadata.

    HTTP/1.1 200 OK
    Content-Length: 113
    Accept-Ranges: bytes
    Content-Type: application/json

    {
      "title" : "My third JSON document",
      "date" : "2018-07-02",
      "note" : "since version 3 notes are allowed"
    }

### Find a Metadata Record

Search will find all current metadata records. There are some filters
available which may be combined. All filters for the metadata records
are set via query parameters. The following filters are allowed:

-   id

-   resourceId

-   from

-   until

> **Note**
>
> The header contains the field 'Content-Range" which displays delivered
> indices and the maximum number of available schema records. If there
> are more than 20 metadata records registered you have to provide page
> and/or size as additional query parameters.

-   page: Number of the page you want to get **(starting with page 0)**

-   size: Number of entries per page.

### Getting a List of all Metadata Records for a Specific Metadata Document

If you want to obtain all versions of a specific resource you may add
'id' as a filter parameter. This may look like this:

    $ curl 'http://localhost:8080/api/v1/metadata?id=ccb78474-b00e-4792-a59d-772c40d037b5' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata?id=ccb78474-b00e-4792-a59d-772c40d037b5 HTTP/1.1
    Host: localhost:8080

As a result, you receive a list of metadata records in descending order.
(current version first)

    HTTP/1.1 200 OK
    Content-Range: 0-19/3
    Content-Type: application/json
    Content-Length: 1580

    [ {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:44.347Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 3,
      "recordVersion" : 3,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=3",
      "documentHash" : "sha1:737762db675032231ac3cb872fccd32a83ac24d1"
    }, {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:44.206Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 2,
      "recordVersion" : 2,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=2",
      "documentHash" : "sha1:737762db675032231ac3cb872fccd32a83ac24d1"
    }, {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:43.979Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 1,
      "recordVersion" : 1,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=1",
      "documentHash" : "sha1:737762db675032231ac3cb872fccd32a83ac24d1"
    } ]

#### Find by resourceId

If you want to find all records belonging to an external resource.
Metastore may hold multiple metadata documents per resource.
(Nevertheless only one per registered schema)

Command line:

    $ curl 'http://localhost:8080/api/v1/metadata?resoureId=anyResourceId' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata?resoureId=anyResourceId HTTP/1.1
    Host: localhost:8080

You will get the current version metadata record.

    HTTP/1.1 200 OK
    Content-Range: 0-19/1
    Content-Type: application/json
    Content-Length: 528

    [ {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:44.347Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 3,
      "recordVersion" : 3,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=3",
      "documentHash" : "sha1:737762db675032231ac3cb872fccd32a83ac24d1"
    } ]

#### Find after a specific date

If you want to find all metadata records updated after a specific date.

Command line:

    $ curl 'http://localhost:8080/api/v1/metadata?from=2021-06-22T12%3A37%3A44.532385Z' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata?from=2021-06-22T12%3A37%3A44.532385Z HTTP/1.1
    Host: localhost:8080

You will get the current version metadata records updated ln the last 2
hours.

    HTTP/1.1 200 OK
    Content-Range: 0-19/1
    Content-Type: application/json
    Content-Length: 528

    [ {
      "id" : "ccb78474-b00e-4792-a59d-772c40d037b5",
      "relatedResource" : "anyResourceId",
      "createdAt" : "2021-06-22T14:37:43Z",
      "lastUpdate" : "2021-06-22T14:37:44.347Z",
      "schemaId" : "my_first_json",
      "schemaVersion" : 3,
      "recordVersion" : 3,
      "acl" : [ {
        "id" : 4,
        "sid" : "SELF",
        "permission" : "ADMINISTRATE"
      } ],
      "metadataDocumentUri" : "http://localhost:8080/api/v1/metadata/ccb78474-b00e-4792-a59d-772c40d037b5?version=3",
      "documentHash" : "sha1:737762db675032231ac3cb872fccd32a83ac24d1"
    } ]

#### Find in a specific date range

If you want to find all metadata records updated in a specific date
range.

Command line:

    $ curl 'http://localhost:8080/api/v1/metadata?from=2021-06-22T12%3A37%3A44.532385Z&until=2021-06-22T13%3A37%3A44.532377Z' -i -X GET

HTTP-wise the call looks as follows:

    GET /api/v1/metadata?from=2021-06-22T12%3A37%3A44.532385Z&until=2021-06-22T13%3A37%3A44.532377Z HTTP/1.1
    Host: localhost:8080

You will get an empty array as no metadata record exists in the given
range:

    HTTP/1.1 200 OK
    Content-Range: 0-19/0
    Content-Type: application/json
    Content-Length: 3

    [ ]

Remarks on Working with Versions
================================

While working with versions you should keep some particularities in
mind. Access to version is only possible for single resources. There is
e.g. no way to obtain all resources in version 2 from the server. If a
specific version of a resource is returned, the obtained ETag also
relates to this specific version. Therefore, you should NOT use this
ETag for any update operation as the operation will fail with response
code 412 (PRECONDITION FAILED). Consequently, it is also NOT allowed to
modify a format version of a resource. If you want to rollback to a
previous version, you should obtain the resource and submit a PUT
request of the entire document which will result in a new version equal
to the previous state unless there were changes you are not allowed to
apply (anymore), e.g. if permissions have changed.
