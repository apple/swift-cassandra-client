# CI-only image: stock Cassandra 3.11 with password authentication enabled.
#
# The stock `cassandra:3.11` image defaults to `AllowAllAuthenticator` and exposes no environment variable
# for the authenticator, so the only way to enable auth is to edit `cassandra.yaml`. Do it at build time
# (a layer, always root) so the container runs the stock entrypoint unchanged. The `grep` fails the build
# if the authenticator line is ever missing/renamed in a future base image, rather than silently booting an
# AllowAll node — `sed` alone exits 0 on no match.
#
# BASE_IMAGE is overridable only so a local build can pull from a fully-qualified registry
# (e.g. `--build-arg BASE_IMAGE=docker.io/library/cassandra:3.11`); CI uses the default.
ARG BASE_IMAGE=cassandra:3.11
FROM ${BASE_IMAGE}
RUN sed -ri 's/^(authenticator:).*/\1 PasswordAuthenticator/' /etc/cassandra/cassandra.yaml \
    && grep -q '^authenticator: PasswordAuthenticator' /etc/cassandra/cassandra.yaml
