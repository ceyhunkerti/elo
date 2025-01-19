Fast data transfer utility for ELT workflows.


> This is not ready for production and still under heavy development.
> For this reason there is only test build instructions.

## Development

- `mv .env.dev .env`
- Download oracle instant client from [here](https://www.oracle.com/database/technologies/instant-client/downloads.html)
- Extract instant client to a path and adjust the `LD_LIBRARY_PATH` in `.env`
- Install [zig](https://ziglang.org). (0.13)
- You may need to install `libaio` required by oracle client.

```sh
sudo apt install libaio1

# if your tests still fail with complaining about missing libaio
# you may also do the following
# adjust the paths according to your os.
# sudo ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1\
```

- Run the test containers with `docker compose up`
- Run the tests with `source .env && zig build test`
