# syntax=docker/dockerfile:1
# Build: full Go toolchain, module and build caches mounted so rebuilds only
# compile what changed. Run: distroless static, nonroot, no shell, no package
# manager; the image holds one static binary. Images pinned by tag and digest.
FROM golang:1.27.4@sha256:5e8afd493a7124a8586e134d9b532731161d4b9becf9dee5a5956b107426f93d AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/app

# Provider double (cmd/fakeprovider) for the compose stack and tests; never
# deployed. Its own build stage, so building the app never compiles it.
FROM build AS build-fakeprovider
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fakeprovider ./cmd/fakeprovider

FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329 AS fakeprovider
COPY --from=build-fakeprovider /out/fakeprovider /fakeprovider
USER nonroot:nonroot
EXPOSE 8090
ENTRYPOINT ["/fakeprovider"]

# The service; last, so a plain `docker build .` still produces it.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329 AS app
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app"]
CMD ["serve"]
