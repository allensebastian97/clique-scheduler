# Build from the repository root (the module replaces the root module with ../..):
#   docker build -f integrations/kai/Dockerfile -t <registry>/scheduler:<kai-version>-nvlinkrack .
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod ./
COPY pkg ./pkg
COPY integrations/kai ./integrations/kai
WORKDIR /src/integrations/kai
RUN go mod download && CGO_ENABLED=0 go build -trimpath -o /out/scheduler ./cmd/kai-scheduler

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/scheduler /usr/local/bin/scheduler
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/scheduler"]
