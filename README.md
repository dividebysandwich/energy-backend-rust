# energy-backend-rust
 A rust version of the energy monitoring backend for Victron inverters

To use, make sure you have set a SSH root password.
Run the program with the following parameters:

```
energy-backend-rust <Victron IP:SSH-Port> <root username> <password>
```

Example: 

```
energy-backend-rust 192.168.178.90:22 root 12345
```

This will require some adaptation. It's a reimplementation of the php-based energy backend. You should check the code and change things as needed, for example whether you want to store data in flat files, use elasticsearch and/or use an external REST service to cache the latest data for use with something like the garmin watch app.

The interesting bit is how to get to the data. I'm using SSH so there's no need to change anything on the Victron's beaglebone filesystem.

Build instructions:

```
cargo build --release
```
