<p align="center">
    <a href="https://surrealdb.com" target="_blank">
        <img width="100%" src="https://github.com/surrealdb/surrealdb/blob/main/img/black/hero.png?raw=true" alt="SurrealDB Hero">
    </a>
</p>

<p align="center">
    <a href="https://github.com/surrealdb/surrealdb"><img src="https://img.shields.io/github/v/release/surrealdb/surrealdb?color=%23ff00a0&include_prereleases&label=version&sort=semver&style=flat-square"></a>
    &nbsp;
    <a href="https://github.com/surrealdb/surrealdb"><img src="https://img.shields.io/badge/built_with-Rust-dca282.svg?style=flat-square"></a>
    &nbsp;
	<a href="https://github.com/surrealdb/surrealdb/actions"><img src="https://img.shields.io/github/actions/workflow/status/surrealdb/surrealdb/ci.yml?style=flat-square&branch=main"></a>
    &nbsp;
    <a href="https://status.surrealdb.com"><img src="https://img.shields.io/uptimerobot/ratio/7/m784409192-e472ca350bb615372ededed7?label=cloud%20uptime&style=flat-square"></a>
    &nbsp;
    <a href="https://hub.docker.com/repository/docker/surrealdb/surrealdb"><img src="https://img.shields.io/docker/pulls/surrealdb/surrealdb?style=flat-square"></a>
    &nbsp;
    <a href="https://github.com/surrealdb/license"><img src="https://img.shields.io/badge/license-BSL_1.1-00bfff.svg?style=flat-square"></a>
</p>

<p align="center">
	<a href="https://surrealdb.com/discord"><img src="https://img.shields.io/discord/902568124350599239?label=discord&style=flat-square&color=5a66f6" alt="Discord"></a>
	&nbsp;
    <a href="https://x.com/surrealdb"><img src="https://img.shields.io/badge/x-follow_us-222222.svg?style=flat-square" alt="X"></a>
    &nbsp;
    <a href="https://dev.to/surrealdb"><img src="https://img.shields.io/badge/dev-join_us-86f7b7.svg?style=flat-square" alt="Dev"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?style=flat-square" alt="LinkedIn"></a>
</p>

<p align="center">
	<a href="https://surrealdb.com/blog"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/blog.svg?raw=true" alt="Blog"></a>
	&nbsp;
	<a href="https://github.com/surrealdb/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/github.svg?raw=true" alt="Github"></a>
	&nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/linkedin.svg?raw=true" alt="LinkedIn"></a>
    &nbsp;
    <a href="https://x.com/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/x.svg?raw=true" alt="X"></a>
    &nbsp;
    <a href="https://www.youtube.com/@surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/youtube.svg?raw=true" alt="YouTube"></a>
    &nbsp;
    <a href="https://dev.to/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/dev.svg?raw=true" alt="Dev"></a>
    &nbsp;
    <a href="https://surrealdb.com/discord"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/discord.svg?raw=true" alt="Discord"></a>
    &nbsp;
    <a href="https://stackoverflow.com/questions/tagged/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/stack-overflow.svg?raw=true" alt="Stack Overflow"></a>
</p>

<br>

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/whatissurreal.svg?raw=true">
&nbsp;&nbsp;What is SurrealDB?</h2>

SurrealDB is an end-to-end cloud native database for web, mobile, serverless,
jamstack, backend, and traditional applications. SurrealDB reduces the
development time of modern applications by simplifying your database and API
stack, removing the need for most server-side components, allowing you to build
secure, performant apps quicker and cheaper. SurrealDB acts as both a database
and a modern, realtime, collaborative API backend layer. SurrealDB can run as a
single server or in a highly-available, highly-scalable distributed mode - with
support for SQL querying from client devices, GraphQL, ACID transactions,
WebSocket connections, structured and unstructured data, graph querying,
full-text indexing, geospatial querying, and row-by-row permissions-based
access.

View the [features](https://surrealdb.com/features), the
latest [releases](https://surrealdb.com/releases), the
product [roadmap](https://surrealdb.com/roadmap),
and [documentation](https://surrealdb.com/docs).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/documentation.svg?raw=true">
&nbsp;&nbsp;Documentation</h2>

For guidance on installation, development, deployment, and administration, see
our [documentation](https://surrealdb.com/docs).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/gettingstarted.svg?raw=true">
&nbsp;&nbsp;Build a Docker image on macOS</h2>

If you build SurrealDB directly on macOS with `cargo build --release`, the
output binary is a macOS `Mach-O` executable. A Linux Docker container cannot
run that binary. For a custom Docker image on macOS, you should therefore build
the Linux binary first and then build the runtime image from that Linux
artifact.

For Apple Silicon, the following flow builds a Linux `arm64` binary and then
produces a runtime image tagged as `psmouz/surrealgql:3.10.0`.

Build the Linux builder image:

```bash
docker build -f docker/Dockerfile --target builder --platform linux/arm64 -t surrealdb-builder:arm64 .
```

Build the Linux SurrealDB binary inside the builder container:

```bash
docker run --rm -t \
  -v "$PWD":/surrealdb \
  surrealdb-builder:arm64 \
  --target aarch64-unknown-linux-gnu \
  --release \
  --locked
```

Verify that the produced binary is a Linux ELF binary:

```bash
file target/aarch64-unknown-linux-gnu/release/surreal
```

Build the runtime image from that Linux binary:

```bash
docker build \
  -f docker/Dockerfile \
  --target prod \
  --platform linux/arm64 \
  --build-arg SURREALDB_BINARY=surreal \
  -t psmouz/surrealgql:3.10.0 \
  target/aarch64-unknown-linux-gnu/release
```

The resulting image can be started directly with Docker:

```bash
docker run --rm -p 8000:8000 psmouz/surrealgql:3.10.0 start --user root --pass root memory
```

If you want to clean up local Rust and Docker build artifacts later while
keeping the `psmouz/surrealgql:3.10.0` image, you can use the following
sequence:

```bash
cargo clean
rm -rf /tmp/surrealdb-cargo*
docker create --name surrealdb-3.10.0-keep psmouz/surrealgql:3.10.0
docker image rm surrealdb-builder:arm64 2>/dev/null || true
docker builder prune -af
```

This removes the local Rust build outputs, temporary Cargo target directories,
the temporary Linux builder image, and unused Docker cache and images, while
keeping `psmouz/surrealgql:3.10.0` referenced by a container so it is not
pruned.

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/gettingstarted.svg?raw=true">
&nbsp;&nbsp;Run using Docker Compose</h2>

The custom image can be used with `docker compose`. The following
`docker-compose.yml` example matches the `psmouz/surrealgql:3.10.0` image built
above and persists a RocksDB datastore to `/Users/psmouz/SurrealDB` on the host.

```yaml
services:
  surrealdb:
    image: psmouz/surrealgql:3.10.0
    container_name: surrealdb-3.10.0
    env_file:
      - .env
    environment:
      - SURREAL_LOG=info # SURREAL_USER and SURREAL_PASS are set in .env
    entrypoint:
      - /surreal
      - start
      - rocksdb:data/data
    volumes:
      - /Users/psmouz/SurrealDB:/data
    ports:
      - 8800:8000
    restart: unless-stopped
```

Start or refresh the service with:

```bash
docker-compose up -d --no-recreate
```

Most of the configuration of SurrealDB can be done
through [environment variables](https://surrealdb.com/docs/surrealdb/cli/env).

You can find a comprehensive list of all the available environment variables in
the help message of the `start` subcommand:

```shell
docker run --rm psmouz/surrealgql:3.10.0 start --help
```

The image contains timezone data. Specify the required timezone with the `TZ`
environment variable:

```bash
docker run -e TZ=Europe/London psmouz/surrealgql:3.10.0 start
```

SurrealDB can be executed as a non-root user for added security. This ensures
that exploiting certain vulnerabilities in the SurrealDB process does not
immediately result in privileged access to the container. When doing this,
ensure that any files required by SurrealDB are mounted to the container in a
volume and that are accessible to that non-root user through their ownership and
permissions.

For the compose example above, that means ensuring the user running the
container can read and write the mounted `/Users/psmouz/SurrealDB` directory.
You can find the UID of the active user in the host by running `id -u`. You can
also provide a group for the container process to run as, such as for example
`user: "1000:1000"`.

The same behavior can be acomplished without Docker Compose by providing the
`-u` or `--user` argument to [
`docker run`](https://docs.docker.com/reference/cli/docker/container/run/).
Similar mechanisms exist in other container management tools such
as [Podman](https://docs.podman.io/en/latest/markdown/podman-run.1.html#user-u-user-group)
or container orchestration systems such
as [Kubernetes](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#set-the-security-context-for-a-pod).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/community.svg?raw=true">
&nbsp;&nbsp;Community</h2>

Join our growing community around the world, for help, ideas, and discussions
regarding SurrealDB.

- View our official [Blog](https://surrealdb.com/blog)
- Chat live with us on [Discord](https://surrealdb.com/discord)
- Follow us on [X](https://x.com/surrealdb)
- Connect with us on [LinkedIn](https://www.linkedin.com/company/surrealdb/)
- Visit us on [YouTube](https://www.youtube.com/@surrealdb)
- Join our [Dev community](https://dev.to/surrealdb)
- Questions tagged #surrealdb
  on [Stack Overflow](https://stackoverflow.com/questions/tagged/surrealdb)

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/contributing.svg?raw=true">
&nbsp;&nbsp;Contributing</h2>

We would
&nbsp;<img width="15" src="https://github.com/surrealdb/surrealdb/blob/main/img/love.svg?raw=true">
&nbsp; for you to get involved with SurrealDB development! If you wish to help,
you can learn more about how you can contribute to this project in
the [contribution guide](../CONTRIBUTING.md).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/security.svg?raw=true">
&nbsp;&nbsp;Security</h2>

For security issues, view
our [vulnerability policy](https://github.com/surrealdb/surrealdb/security/policy),
view our [security policy](https://surrealdb.com/legal/security), and kindly
email us at [security@surrealdb.com](mailto:security@surrealdb.com) instead of
posting a public issue on GitHub.

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/license.svg?raw=true">
&nbsp;&nbsp;License</h2>

Source code for SurrealDB is variously licensed under a number of different
licenses. A copy of each license can be found
in [each repository](https://github.com/surrealdb).

- Libraries and SDKs, each located in its own distinct repository, are released
  under either
  the [Apache License 2.0](https://github.com/surrealdb/license/blob/main/APL.txt)
  or [MIT License](https://github.com/surrealdb/license/blob/main/MIT.txt).
- Certain core database components, each located in its own distinct repository,
  are released under
  the [Apache License 2.0](https://github.com/surrealdb/license/blob/main/APL.txt).
- Core database code for SurrealDB, located
  in [this repository](https://github.com/surrealdb/surrealdb), is released
  under the [Business Source License 1.1](/LICENSE).

For more information, see
the [licensing information](https://github.com/surrealdb/license).
