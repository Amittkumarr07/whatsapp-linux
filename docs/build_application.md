# To build a debian packages 
The steps are below: 

# Build release binaries
``` bash 
cargo build --workspace --release
```

# Create the binary installation directory
``` bash
mkdir -p package-folder/usr/bin
```

# Copy both binaries
``` bash
install -m 755 target/release/whatsapp-ui package-folder/usr/bin/
install -m 755 target/release/whatsapp-daemon package-folder/usr/bin/
```

``` bash
# Build the Debian package
dpkg-deb --build --root-owner-group package-folder whatsapp-linux_0.1.0_amd64.deb
```