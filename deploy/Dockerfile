FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/relay ./cmd/relay \
 && CGO_ENABLED=0 go build -o /out/demo-receiver ./cmd/demo-receiver \
 && CGO_ENABLED=0 go build -o /out/reconcile ./cmd/reconcile

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
ENTRYPOINT ["relay"]
CMD ["all"]
