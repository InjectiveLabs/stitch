FROM golang:1.25-alpine AS build
WORKDIR /src
RUN apk add --no-cache git make
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
RUN go build -mod=readonly -trimpath \
    -ldflags "-w -s \
        -X 'github.com/InjectiveLabs/stitch/internal/cmd.version=${VERSION}' \
        -X 'github.com/InjectiveLabs/stitch/internal/cmd.commit=${COMMIT}'" \
    -o /out/stitch ./cmd/stitch

# Distroless for a small, hardened runtime image. Stitch reads no
# files at runtime beyond the config, so static plus distroless is fine.
FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=build /out/stitch /usr/local/bin/stitch
USER nonroot:nonroot
EXPOSE 5001 5002 5003 5005 5006 5007 5008 9091
ENTRYPOINT ["/usr/local/bin/stitch"]
CMD ["start", "--config", "/etc/stitch/config.yaml"]
