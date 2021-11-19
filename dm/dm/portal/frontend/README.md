# DM Web Interface

## How to Run

### Run dm-portal

in the repo root folder:

    $ make dm-portal
    $ ./bin/dm-portal -port 4001 -task-file-path /tmp

### Run dm-fe

in the repo `dm/dm/portal/frontend` folder:

1. `yarn install`
1. `yarn start`

Note: you must run dm-portal in 4001 port as backend service first.

build:

1. `yarn build`
