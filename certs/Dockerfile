FROM alpine:3.20
RUN apk add --no-cache openssl
COPY generate-certs.sh /usr/local/bin/generate-certs.sh
ENTRYPOINT ["sh", "/usr/local/bin/generate-certs.sh"]
