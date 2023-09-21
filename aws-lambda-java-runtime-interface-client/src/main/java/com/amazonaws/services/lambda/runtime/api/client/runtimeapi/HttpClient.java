package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import java.util.Map;
import java.util.Optional;
import java.util.HashMap;
import java.io.IOException;

public interface HttpClient {
    InvocationRequest waitForNextInvocation(String path);
    void postInvocationResponse(String path, byte[] requestId, byte[] response);
    HttpResponse post(String path, byte[] response);

    public class ResponseResolver<T> {
        private final Class<T> clazz;
        private final HttpResponse response;

        public ResponseResolver(Class<T> clazz, HttpResponse response) {
            this.clazz = clazz;
            this.response = response;
        }

        public HttpResponse resolve() throws IOException {
            return null;
        }
    }

    public class HttpResponse {
        private static final String REQUEST_ID_HEADER = "Lambda-Runtime-Aws-Request-Id";
        private static final String FUNCTION_ARN_HEADER = "Lambda-Runtime-Invoked-Function-Arn";
        private static final String DEADLINE_MS_HEADER = "Lambda-Runtime-Deadline-Ms";
        private static final String TRACE_ID_HEADER = "Lambda-Runtime-Trace-Id";
        private static final String CLIENT_CONTEXT_HEADER = "Lambda-Runtime-Client-Context";
        private static final String COGNITO_IDENTITY_HEADER = "Lambda-Runtime-Cognito-Identity";
    
        private final int statusCode;
        private final byte[] content;
        private Map<String, String[]> headers;        

        public HttpResponse(int statusCode, byte[] content) {
            this.statusCode = statusCode;
            this.content = content;
            this.headers = new HashMap<>();
        }

        public HttpResponse(int statusCode, byte[] content, Map<String, String[]> headers) {
            this(statusCode, content);
            this.headers = headers;
        }

        public void addHeader(String key, String value) {
            headers.put(key, new String[] { value });
        }
        
        public String[] getHeader(String key) {
            return headers.get(key);
        }

        public Map<String, String[]> getHeaders() {
            return headers;
        }

        public void setHeaders(Map<String, String[]> headers) {
            this.headers = headers;
        }

        public void removeHeader(String key) {
            headers.remove(key);
        }

        public int getStatusCode() {
            return statusCode;
        }

        public byte[] getContent() {
            return content;
        }

        public InvocationRequest getInvocationRequest() {
            InvocationRequest request = new InvocationRequest();

            request.setContent(content);

            request.id = null;
            request.invokedFunctionArn = null;
            request.deadlineTimeInMs = 0;
            request.xrayTraceId = null;
            request.clientContext = null;
            request.cognitoIdentity = null;

            for (Map.Entry<String,String[]> header : headers.entrySet()) {
                switch (header.getKey()) {
                    case REQUEST_ID_HEADER:
                        request.id = header.getValue()[0];
                        break;
                
                    case FUNCTION_ARN_HEADER:
                        request.invokedFunctionArn = header.getValue()[0];
                        break;
                
                    case DEADLINE_MS_HEADER:
                        request.deadlineTimeInMs = Long.parseLong(header.getValue()[0]);
                        break;
                
                    case TRACE_ID_HEADER:
                        request.xrayTraceId = header.getValue()[0];
                        break;
                
                    case CLIENT_CONTEXT_HEADER:
                        request.clientContext = header.getValue()[0];
                        break;
                
                    case COGNITO_IDENTITY_HEADER:
                        request.cognitoIdentity = header.getValue()[0];
                        break;
                
                    default:
                        break;
                }
            }

            Optional.ofNullable(request.id)
                    .orElseThrow(() -> new LambdaRuntimeClientException("Request ID absent"));
            Optional.ofNullable(request.invokedFunctionArn)
                    .orElseThrow(() -> new LambdaRuntimeClientException("Function ARN absent"));

            return request;
        }


    }
}