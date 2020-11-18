<!-- Your testing strategy/code should be put into this directory.

Put any scripts or source code here.

If you used a combination of 'printf' and 'nc' commands or other similar command-line approaches
for testing your code, put them in a file (e.g. TESTING.txt, TESTING.md, etc).  Please
indicate within your code or write-up which tests are associated with each rubric item.

Thank you. -->

# Testing Strategies
We wrote out testing script in Node.js, and we devided the test cases into four files: `basic.test.js`, `subdir.test.js`, and `mime-types.test.js`, and `concurr-pipe.test.js`. 


To set up this testing program, run the following command:
`$ npm install`

To run the test cases, run the following command:
`$ npm run test`

The testing result is stored in `report.html`.

## basic.test.js
In this file, we tested cases including: valid format, find valid files, and timeout/close.

### valid format

1. should respond nothing when sending nothing.
    - After a client connected to the server, the client did nothing. We expected the connection was established and nothing in response.

2. should respond correct HTTP version in header.
    - After a client connected to the server, the client sent a http request of `/`. We expected the response header contained `HTTP/1.1` version.

3. should respond 400 when Slash Missing.
    - After a client connected to the server, the client sent a http request with a `missing slash URL`. We expected the response contained 400 code.

4. should respond 400 when sending unspported HTTP version.
    - After a client connected to the server, the client sent a http request with `HTTP/1.0` in initial line. We expected the response contained 400 code.

5. sshould respond 400 when sending unsupported HTTP method.
    - After a client connected to the server, the client sent a http request with `POST` in initial line. We expected the response contained 400 code.

6. should respond 400 when Host is not presented request.
    - After a client connected to the server, the client sent a http request without `Host` in headers. We expected the response contained 400 code.

7. should respond 400 when sending empty format request.
    - After a client connected to the server, the client sent a http request with only `\r\n` in initial line. We expected the response contained 400 code.


### find valid files

1. should respond 200 and correct content when file exists.
    - After a client connected to the server, the client sent a http request of a existed file. We expected the response contained 200 code and correct file content.

2. should respond 404 when file not exists.
    - After a client connected to the server, the client sent a http request of a non-existed file. We expected the response contained 404 code.

3. should respond 200 and correct content when a large file exists.
    - After a client connected to the server, the client sent a http request of a large file (10 MB). We expected the response contained 200 code and correct large file content (10 MB).

4. should respond 200 and correct content when an empty file exists.
    - After a client connected to the server, the client sent a http request of an empty file. We expected the response contained 200 code and correct empty content.

5. should respond the existed file content with correct content when there is a large request.
    - After a client connected to the server, the client sent a http request with 100000 headers. We expected the response contained 200 code and correct file content.

### timeout/close

1. should respond 400 and close connection when there is partial request.
    - After a client connected to the server, the client sent a partial http request and waited for the timeout. We expected the response contained 400 with connection closed.

2. should close connection after timeout without response when no partial request sent.
    - After a client connected to the server, the client did nothing and waited for the timeout. We expected the sever closed the connection without sending anything to the client.

3. should end connection when the client close connection.
    - After a client connected to the server, the client shut down the connection. We expected the connection was closed.

4. should keep the connection when "Connection: close" is not in the request header.
    - After a client connected to the server, the client sent a http request without "Connection: close" in header. We expected the connection was not closed.

5. should close the connection when "Connection: close" is in the request header.
    - After a client connected to the server, the client sent a http request with "Connection: close" in header. We expected the connection was closed and "Connection: close" is set in the response header.

## subdir.test.js

### file paths

1. should respond 200 when file exists.
    - After a client connected to the server, the client sent a http request of a existed file. We expected the response contained 200 code.

2. should respond 200 when file exists in subdir.
    - After a client connected to the server, the client sent a http request of a existed file in a subdirectory. We expected the response contained 200 code and correct file content.

3. should respond 404 when file does not exists in subdir.
    - After a client connected to the server, the client sent a http request of a non-existed file in a subdirectory. We expected the response contained 404 code.

4. should respond 404 when subdir does not exist.
    - After a client connected to the server, the client sent a http request of a file in a non-existed subdirectory. We expected the response contained 404 code.

5. should redirect to index.html when path is a dir.
    - After a client connected to the server, the client sent a http request of directory URL. We expected the response contained 200 code and correct file content of `index.html`.

6. should redirect to subdir/index.html when path is a subdir.
    - After a client connected to the server, the client sent a http request of subdirectory URL. We expected the response contained 200 code and correct file content of `subdir/index.html`.

7. should support relative path.
    - After a client connected to the server, the client sent a http request with `/dir1/../root.txt` URL. We expected the response contained 200 code and correct file content.

8. should support relative path traversal.
    - After a client connected to the server, the client sent a http request with `/dir1/../dir2/file2.txt` URL. We expected the response contained 200 code and correct file content.

9. should support complicated relative path traversal.
    - After a client connected to the server, the client sent a http request with `/dir1/../dir2/../dir1/dir11/../dir12/../../root.txt` URL. We expected the response contained 200 code and correct file content.

10. should respond 404 when path escape document root.
    - After a client connected to the server, the client sent a http request with `/dir1/../../` URL which escaped the document root. We expected the response contained 404 code.

11. should respond 404 when path escape document root (escape filesystem root).
    - After a client connected to the server, the client sent a http request with `/dir1/../../../../../../../../../../../../../../../../` URL which escaped the filesystem root. We expected the response contained 404 code.

12. should support escape doc root and traverse back to root.
    - After a client connected to the server, the client sent a http request with `/dir1/../../SERVER_ROOT/dir1/file1.txt` URL which escape doc root but traverse back to root. We expected the response contained 200 code and correct file content.


## concurrency.test.js

1. should respond file content with correct content when files exists.
    - After four clients connected to the server, each client sent a http request of existed files. We expected the response contained 200 code and correct content for these clients.

2. should respond 400 and close connections when there is malformed request.
    - After four clients connected to the server, two clients sent a http request of different existed files and the others sent malformed requests. We expected the response contained 200 code and correct content for correct request clients, and close connection with a 400 code response for the others.

## pipeline.test.js

1. should respond correct content when a client send multiple requests and close connection after timeout.
    - After a client connected to the server, the client sent four consecutive http requests, and the client waited until timeout. We expected response contained 200 code and correct file content, and the connection should closed eventually.

2. should respond 404 when file does not exist
    - After a client connected to the server, the client sent four consecutive http requests where two of which contained non-existed files, and the client waited until timeout. We expected response contained 200 code and correct file content for existed files and 404 code for non-existed fils, and the connection should closed eventually.

3. should close connection when the second request is malformed .
    - After a client connected to the server, the client sent two consecutive http requests, which the first one request an existed file and the last request was malformed. We expected first response contained 200 code and correct file content; the last response contained 400 code and closed the connection. 

4. should not process the next request after connection close.
    - After a client connected to the server, the client sent three consecutive http requests, while the second request was malformed and the others were existed files. We expected first response contained 200 code and correct file content; the second response contained 400 code and closed the connection, and while there was no response for the last request.


## mime-types.test.js

### support all MIME types

1. should respond with content-type (basic).
    - After a client connected to the server, the client sent a valid http request of `/test.zip`. We expected the response contained correct content-type.

2. should respond content-type for unknown file extension.
    - After a client connected to the server, the client sent a valid http request of non-supported type `/test.xxxx`. We expected the response respond correct content-type as `application/octet-stream`.

3. should respond with content-type (pipeline & comprehensive), i.e., all the data types in MIME types.
    - After a client connected to the server, the client sent valid http requests of all data type in MIME. We expected the response respond correct content-type and content.
