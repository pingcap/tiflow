# DM Web UI

Web UI for TiDB Data Migration Platform, built with TypeScript and React.

## Development

To start to develop it you will need Nodejs environment ready and yarn installed as package manager, following scripts will help you to get started:

```bash
 # install dependencies
yarn install

# start the development server
yarn start

# build and output static files to `dist` directory
yarn build
```

See `package.json` for more scripts.

By default, it talks to local DM Server through `http://localhost:8261/api/v1/...`, so you will need to have it running in your machine first.

Make sure you have Golang installed since we'll need to build the server binaries from source, at the root of this project, run `make dm-worker` and `make dm-master` to get them.

Create conf file for them, there are plenty of examples in `dm/tests/openapi/conf` directory. The `openapi` option is still experimental and is off by default so make sure you set it to true.

Then run master and worker with configs you just created respectively. For example:

```bash
./bin/dm-master -config path/to/conf.yaml
./bin/dm-worker -config path/to/conf.yaml
```

Now you can start the front-end development server with `yarn start`!.
