# ---- build ----
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/booking ./cmd/booking \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stampede ./cmd/stampede

# ---- runtime: no shell, no package manager, non-root ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/booking /booking
COPY --from=build /out/stampede /stampede
USER nonroot
EXPOSE 8081
ENTRYPOINT ["/booking"]
CMD ["api"]
